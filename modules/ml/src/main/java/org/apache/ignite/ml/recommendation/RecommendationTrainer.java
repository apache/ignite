/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.recommendation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.recommendation.util.MatrixFactorizationGradient;
import org.apache.ignite.ml.recommendation.util.RecommendationBinaryDatasetDataBuilder;
import org.apache.ignite.ml.recommendation.util.RecommendationDatasetData;
import org.apache.ignite.ml.recommendation.util.RecommendationDatasetDataBuilder;

/**
 * Trainer of the recommendation system.
 */
public class RecommendationTrainer {
    /** Environment builder. */
    private LearningEnvironmentBuilder environmentBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Trainer learning environment. */
    private LearningEnvironment trainerEnvironment = environmentBuilder.buildForTrainer();

    /** Batch size of stochastic gradient descent. The size of a dataset used on each step of SGD. */
    private int batchSize = 1000;

    /** Regularization parameter. */
    private double regParam;

    /** Learning rate. */
    private double learningRate = 10.0;

    /** Max number of SGD iterations. */
    private int maxIterations = 1000;

    /** Minimal improvement of the model to continue training. */
    private double minMdlImprovement;

    /** Number of rows/cols in matrices after factorization. */
    private int k = 10;

    /**
     * Fits prediction model on a data stored in binary format.
     *
     * @param datasetBuilder Dataset builder.
     * @param objFieldName Object field name.
     * @param subjFieldName Subject field name.
     * @param ratingFieldName Rating field name.
     * @return Trained recommendation model.
     */
    public RecommendationModel<Serializable, Serializable> fit(DatasetBuilder<Object, BinaryObject> datasetBuilder,
        String objFieldName, String subjFieldName, String ratingFieldName) {
        return update(datasetBuilder, objFieldName, subjFieldName, ratingFieldName, null);
    }

    /**
     * Updates prediction model on a data stored in binary format.
     *
     * @param datasetBuilder Dataset builder.
     * @param objFieldName Object field name.
     * @param subjFieldName Subject field name.
     * @param ratingFieldName Rating field name.
     * @param mdl Previous model.
     * @return Trained recommendation model.
     */
    public RecommendationModel<Serializable, Serializable> update(DatasetBuilder<Object, BinaryObject> datasetBuilder,
        String objFieldName, String subjFieldName, String ratingFieldName,
        RecommendationModel<Serializable, Serializable> mdl) {
        try (Dataset<EmptyContext, RecommendationDatasetData<Serializable, Serializable>> dataset = datasetBuilder.build(
            environmentBuilder,
            new EmptyContextBuilder<>(),
            new RecommendationBinaryDatasetDataBuilder(objFieldName, subjFieldName, ratingFieldName),
            trainerEnvironment
        )) {
            return train(dataset, mdl);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fits prediction model.
     *
     * @param datasetBuilder Dataset builder.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <O> Type of an object.
     * @param <S> Type of a subject.
     * @return Trained recommendation model.
     */
    public <K, O extends Serializable, S extends Serializable> RecommendationModel<O, S> fit(
        DatasetBuilder<K, ? extends ObjectSubjectRatingTriplet<O, S>> datasetBuilder) {
        return update(datasetBuilder, null);
    }

    /**
     * Fits prediction model.
     *
     * @param datasetBuilder Dataset builder.
     * @param mdl Previous model.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <O> Type of an object.
     * @param <S> Type of a subject.
     * @return Trained recommendation model.
     */
    public <K, O extends Serializable, S extends Serializable> RecommendationModel<O, S> update(
        DatasetBuilder<K, ? extends ObjectSubjectRatingTriplet<O, S>> datasetBuilder, RecommendationModel<O, S> mdl) {
        try (Dataset<EmptyContext, RecommendationDatasetData<O, S>> dataset = datasetBuilder.build(
            environmentBuilder,
            new EmptyContextBuilder<>(),
            new RecommendationDatasetDataBuilder<>(),
            trainerEnvironment
        )) {
            return train(dataset, mdl);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Trains recommendation model using specified dataset.
     *
     * @param dataset Partition based dataset.
     * @param <O> Type of an object.
     * @param <S> Type of a subject.
     * @return Trained recommendation model.
     */
    private <O extends Serializable, S extends Serializable> RecommendationModel<O, S> train(
        Dataset<EmptyContext, RecommendationDatasetData<O, S>> dataset, RecommendationModel<O, S> mdl) {
        // Collect total set of objects and subjects (their identifiers).
        Set<O> objects = dataset.compute(RecommendationDatasetData::getObjects, RecommendationTrainer::join);
        Set<S> subjects = dataset.compute(RecommendationDatasetData::getSubjects, RecommendationTrainer::join);

        // Generate initial model (object and subject matrices) initializing them with random values.
        Map<O, Vector> objMatrix = mdl == null ?
            generateRandomVectorForEach(objects, trainerEnvironment.randomNumbersGenerator()) :
            new HashMap<>(mdl.getObjMatrix());
        Map<S, Vector> subjMatrix = mdl == null ?
            generateRandomVectorForEach(subjects, trainerEnvironment.randomNumbersGenerator()) :
            new HashMap<>(mdl.getSubjMatrix());

        if (mdl != null) {
            for (O o : objects) {
                if (!objMatrix.containsKey(o))
                    objMatrix.put(o, randomVector(k, trainerEnvironment.randomNumbersGenerator()));
            }

            for (S s : subjects) {
                if (!subjMatrix.containsKey(s))
                    subjMatrix.put(s, randomVector(k, trainerEnvironment.randomNumbersGenerator()));
            }
        }

        // SGD steps.
        for (int i = 0; maxIterations == -1 || i < maxIterations; i++) {
            int seed = i;

            // Calculate gradient on reach partition and aggregate results.
            MatrixFactorizationGradient<O, S> grad = dataset.compute(
                (data, env) -> data.calculateGradient(
                    objMatrix,
                    subjMatrix,
                    batchSize,
                    seed ^ env.partition(),
                    regParam,
                    learningRate
                ),
                RecommendationTrainer::sum
            );

            if (minMdlImprovement != 0 && calculateImprovement(grad) < minMdlImprovement)
                break;

            // Apply aggregated gradient.
            grad.applyGradient(objMatrix, subjMatrix);
        }

        return new RecommendationModel<>(objMatrix, subjMatrix);
    }

    /**
     * Calculates improvement of the model that corresponds to the specified gradient (how significantly model will be
     * improved after the gradient application).
     *
     * @param grad Matrix factorization gradient.
     * @param <O> Type of an object.
     * @param <S> Type of a subject.
     * @return Measure of model improvement correspondent to the specified gradient.
     */
    private <O extends Serializable, S extends Serializable> double calculateImprovement(
        MatrixFactorizationGradient<O, S> grad) {
        double mean = 0;

        for (Vector vector : grad.getObjGrad().values()) {
            for (int i = 0; i < vector.size(); i++)
                mean += Math.abs(vector.get(i));
        }

        for (Vector vector : grad.getSubjGrad().values()) {
            for (int i = 0; i < vector.size(); i++)
                mean += Math.abs(vector.get(i));
        }

        mean /= (grad.getSubjGrad().size() + grad.getObjGrad().size());

        return mean;
    }

    /**
     * Generates a random vector with length {@link #k} for each given object.
     *
     * @param objects Collection of object.
     * @param rnd Random generator.
     * @param <T> Type of an object.
     * @return Pairs of objects and generated vectors.
     */
    private <T> Map<T, Vector> generateRandomVectorForEach(Collection<T> objects, Random rnd) {
        Map<T, Vector> res = new HashMap<>();
        for (T obj : objects)
            res.put(obj, randomVector(k, rnd));

        return res;
    }

    /**
     * Joins two sets ({@code null} values are acceptable).
     *
     * @param a First set.
     * @param b Second set.
     * @param <T> Type of set elements.
     * @return Joined set.
     */
    private static <T> Set<T> join(Set<T> a, Set<T> b) {
        if (a == null)
            return b;

        if (b != null)
            a.addAll(b);

        return a;
    }

    /**
     * Set up learning environment builder.
     *
     * @param environmentBuilder Learning environment builder.
     * @return This object.
     */
    public RecommendationTrainer withLearningEnvironmentBuilder(LearningEnvironmentBuilder environmentBuilder) {
        this.environmentBuilder = environmentBuilder;

        return this;
    }

    /**
     * Set up trainer learning environment.
     *
     * @param trainerEnvironment Trainer learning environment.
     * @return This object.
     */
    public RecommendationTrainer withTrainerEnvironment(LearningEnvironment trainerEnvironment) {
        this.trainerEnvironment = trainerEnvironment;

        return this;
    }

    /**
     * Set up batch size parameter.
     *
     * @param batchSize Batch size of stochastic gradient descent. The size of a dataset used on each step of SGD.
     * @return This object.
     */
    public RecommendationTrainer withBatchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    /**
     * Set up regularization parameter.
     *
     * @param regParam Regularization parameter.
     * @return This object.
     */
    public RecommendationTrainer withRegularization(double regParam) {
        this.regParam = regParam;

        return this;
    }

    /**
     * Set up learning rate parameter.
     *
     * @param learningRate Learning rate.
     * @return This object.
     */
    public RecommendationTrainer withLearningRate(double learningRate) {
        this.learningRate = learningRate;

        return this;
    }

    /**
     * Set up max iterations parameter.
     *
     * @param maxIterations Max iterations.
     * @return This object.
     */
    public RecommendationTrainer withMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;

        return this;
    }

    /**
     * Set up {@code minModelImprovement} parameter (minimal improvement of the model to continue training).
     *
     * @param minMdlImprovement Minimal improvement of the model to continue training.
     * @return This object.
     */
    public RecommendationTrainer withMinMdlImprovement(double minMdlImprovement) {
        this.minMdlImprovement = minMdlImprovement;

        return this;
    }

    /**
     * Set up {@code k} parameter (number of rows/cols in matrices after factorization).
     *
     * @param k Number of rows/cols in matrices after factorization
     * @return This object.
     */
    public RecommendationTrainer withK(int k) {
        this.k = k;

        return this;
    }

    /**
     * Returns sum of two matrix factorization gradients.
     *
     * @param a First gradient.
     * @param b Second gradient.
     * @param <O> Type of object.
     * @param <S> Type ot subject.
     * @return Sum of two matrix factorization gradients.
     */
    private static <O extends Serializable, S extends Serializable> MatrixFactorizationGradient<O, S> sum(
        MatrixFactorizationGradient<O, S> a,
        MatrixFactorizationGradient<O, S> b) {
        return new MatrixFactorizationGradient<>(
            sum(a == null ? null : a.getObjGrad(), b == null ? null : b.getObjGrad()),
            sum(a == null ? null : a.getSubjGrad(), b == null ? null : b.getSubjGrad()),
            (a == null ? 0 : a.getRows()) + (b == null ? 0 : b.getRows())
        );
    }

    /**
     * Returns sum of two matrices.
     *
     * @param a First matrix.
     * @param b Second matrix.
     * @param <T> Type of a key.
     * @return Sum of two matrices.
     */
    private static <T> Map<T, Vector> sum(Map<T, Vector> a, Map<T, Vector> b) {
        if (a == null)
            return b;

        if (b == null)
            return a;

        Map<T, Vector> res = new HashMap<>();

        for (Map<T, Vector> map : Arrays.asList(a, b)) {
            for (Map.Entry<T, Vector> e : map.entrySet()) {
                Vector vector = res.get(e.getKey());
                res.put(e.getKey(), vector == null ? e.getValue() : e.getValue().plus(vector));
            }
        }

        return Collections.unmodifiableMap(res);
    }

    /**
     * Generates a new randomized vector with length {@code k} and max value {@code max}.
     *
     * @param k Cardinality of the vector.
     * @param rnd Random.
     * @return Randomized vector.
     */
    private static Vector randomVector(int k, Random rnd) {
        double[] vector = new double[k];
        for (int i = 0; i < vector.length; i++)
            vector[i] = rnd.nextDouble();

        return VectorUtils.of(vector);
    }
}
