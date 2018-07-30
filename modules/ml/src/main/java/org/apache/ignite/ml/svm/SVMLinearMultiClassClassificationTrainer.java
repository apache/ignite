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

package org.apache.ignite.ml.svm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Base class for a soft-margin SVM linear multiclass-classification trainer based on the communication-efficient
 * distributed dual coordinate ascent algorithm (CoCoA) with hinge-loss function.
 *
 * All common parameters are shared with bunch of binary classification trainers.
 */
public class SVMLinearMultiClassClassificationTrainer
    extends SingleLabelDatasetTrainer<SVMLinearMultiClassClassificationModel> {
    /** Amount of outer SDCA algorithm iterations. */
    private int amountOfIterations = 20;

    /** Amount of local SDCA algorithm iterations. */
    private int amountOfLocIterations = 50;

    /** Regularization parameter. */
    private double lambda = 0.2;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @return Model.
     */
    @Override public <K, V> SVMLinearMultiClassClassificationModel fit(DatasetBuilder<K, V> datasetBuilder,
                                                                IgniteBiFunction<K, V, Vector> featureExtractor,
                                                                IgniteBiFunction<K, V, Double> lbExtractor) {
        List<Double> classes = extractClassLabels(datasetBuilder, lbExtractor);

        SVMLinearMultiClassClassificationModel multiClsMdl = new SVMLinearMultiClassClassificationModel();

        classes.forEach(clsLb -> {
            SVMLinearBinaryClassificationTrainer trainer = new SVMLinearBinaryClassificationTrainer()
                .withAmountOfIterations(this.amountOfIterations())
                .withAmountOfLocIterations(this.amountOfLocIterations())
                .withLambda(this.lambda());

            IgniteBiFunction<K, V, Double> lbTransformer = (k, v) -> {
                Double lb = lbExtractor.apply(k, v);

                if (lb.equals(clsLb))
                    return 1.0;
                else
                    return -1.0;
            };
            multiClsMdl.add(clsLb, trainer.fit(datasetBuilder, featureExtractor, lbTransformer));
        });

        return multiClsMdl;
    }

    /** Iterates among dataset and collects class labels. */
    private <K, V> List<Double> extractClassLabels(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Double> lbExtractor) {
        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, EmptyContext, LabelPartitionDataOnHeap> partDataBuilder = new LabelPartitionDataBuilderOnHeap<>(lbExtractor);

        List<Double> res = new ArrayList<>();

        try (Dataset<EmptyContext, LabelPartitionDataOnHeap> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            partDataBuilder
        )) {
            final Set<Double> clsLabels = dataset.compute(data -> {
                final Set<Double> locClsLabels = new HashSet<>();

                final double[] lbs = data.getY();

                for (double lb : lbs) locClsLabels.add(lb);

                return locClsLabels;
            }, (a, b) -> a == null ? b : Stream.of(a, b).flatMap(Collection::stream).collect(Collectors.toSet()));

            res.addAll(clsLabels);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    /**
     * Set up the regularization parameter.
     *
     * @param lambda The regularization parameter. Should be more than 0.0.
     * @return Trainer with new lambda parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer  withLambda(double lambda) {
        assert lambda > 0.0;
        this.lambda = lambda;
        return this;
    }

    /**
     * Gets the regularization lambda.
     *
     * @return The parameter value.
     */
    public double lambda() {
        return lambda;
    }

    /**
     * Gets the amount of outer iterations of SCDA algorithm.
     *
     * @return The parameter value.
     */
    public int amountOfIterations() {
        return amountOfIterations;
    }

    /**
     * Set up the amount of outer iterations of SCDA algorithm.
     *
     * @param amountOfIterations The parameter value.
     * @return Trainer with new amountOfIterations parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer  withAmountOfIterations(int amountOfIterations) {
        this.amountOfIterations = amountOfIterations;
        return this;
    }

    /**
     * Gets the amount of local iterations of SCDA algorithm.
     *
     * @return The parameter value.
     */
    public int amountOfLocIterations() {
        return amountOfLocIterations;
    }

    /**
     * Set up the amount of local iterations of SCDA algorithm.
     *
     * @param amountOfLocIterations The parameter value.
     * @return Trainer with new amountOfLocIterations parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer  withAmountOfLocIterations(int amountOfLocIterations) {
        this.amountOfLocIterations = amountOfLocIterations;
        return this;
    }
}
