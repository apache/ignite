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

package org.apache.ignite.ml.regressions.logistic.multiclass;

import java.io.Serializable;
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
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.structures.partition.LabelPartitionDataOnHeap;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * All common parameters are shared with bunch of binary classification trainers.
 */
public class LogRegressionMultiClassTrainer<P extends Serializable>
    extends SingleLabelDatasetTrainer<LogRegressionMultiClassModel> {
    /** Update strategy. */
    private UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy;

    /** Max number of iteration. */
    private int amountOfIterations;

    /** Batch size. */
    private int batchSize;

    /** Number of local iterations. */
    private int amountOfLocIterations;

    /** Seed for random generator. */
    private long seed;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @return Model.
     */
    @Override public <K, V> LogRegressionMultiClassModel fit(DatasetBuilder<K, V> datasetBuilder,
                                                                IgniteBiFunction<K, V, Vector> featureExtractor,
                                                                IgniteBiFunction<K, V, Double> lbExtractor) {
        List<Double> classes = extractClassLabels(datasetBuilder, lbExtractor);

        LogRegressionMultiClassModel multiClsMdl = new LogRegressionMultiClassModel();

        classes.forEach(clsLb -> {
            LogisticRegressionSGDTrainer<?> trainer =
                new LogisticRegressionSGDTrainer<>(updatesStgy, amountOfIterations, batchSize, amountOfLocIterations, seed);

            IgniteBiFunction<K, V, Double> lbTransformer = (k, v) -> {
                Double lb = lbExtractor.apply(k, v);

                if (lb.equals(clsLb))
                    return 1.0;
                else
                    return 0.0;
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
     * @param batchSize The size of learning batch.
     * @return Trainer with new batch size parameter value.
     */
    public LogRegressionMultiClassTrainer withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the batch size.
     *
     * @return The parameter value.
     */
    public double batchSize() {
        return batchSize;
    }

    /**
     * Gets the amount of outer iterations of SGD algorithm.
     *
     * @return The parameter value.
     */
    public int amountOfIterations() {
        return amountOfIterations;
    }

    /**
     * Set up the amount of outer iterations.
     *
     * @param amountOfIterations The parameter value.
     * @return Trainer with new amountOfIterations parameter value.
     */
    public LogRegressionMultiClassTrainer withAmountOfIterations(int amountOfIterations) {
        this.amountOfIterations = amountOfIterations;
        return this;
    }

    /**
     * Gets the amount of local iterations.
     *
     * @return The parameter value.
     */
    public int amountOfLocIterations() {
        return amountOfLocIterations;
    }

    /**
     * Set up the amount of local iterations of SGD algorithm.
     *
     * @param amountOfLocIterations The parameter value.
     * @return Trainer with new amountOfLocIterations parameter value.
     */
    public LogRegressionMultiClassTrainer withAmountOfLocIterations(int amountOfLocIterations) {
        this.amountOfLocIterations = amountOfLocIterations;
        return this;
    }

    /**
     * Set up the regularization parameter.
     *
     * @param seed Seed for random generator.
     * @return Trainer with new seed parameter value.
     */
    public LogRegressionMultiClassTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * Gets the seed for random generator.
     *
     * @return The parameter value.
     */
    public long seed() {
        return seed;
    }

    /**
     * Set up the regularization parameter.
     *
     * @param updatesStgy Update strategy.
     * @return Trainer with new update strategy parameter value.
     */
    public LogRegressionMultiClassTrainer withUpdatesStgy(UpdatesStrategy updatesStgy) {
        this.updatesStgy = updatesStgy;
        return this;
    }

    /**
     * Gets the update strategy..
     *
     * @return The parameter value.
     */
    public UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy() {
        return updatesStgy;
    }
}
