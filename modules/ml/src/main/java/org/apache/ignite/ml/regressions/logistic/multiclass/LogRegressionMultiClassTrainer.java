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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
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
    private UpdatesStrategy updatesStgy = new UpdatesStrategy<>(
        new SimpleGDUpdateCalculator(0.2),
        SimpleGDParameterUpdate::sumLocal,
        SimpleGDParameterUpdate::avg
    );

    /** Max number of iteration. */
    private int amountOfIterations = 100;

    /** Batch size. */
    private int batchSize = 100;

    /** Number of local iterations. */
    private int amountOfLocIterations = 100;

    /** Seed for random generator. */
    private long seed = 1234L;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return Model.
     */
    @Override public <K, V> LogRegressionMultiClassModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {
        List<Double> classes = extractClassLabels(datasetBuilder, lbExtractor);

        return updateModel(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override public <K, V> LogRegressionMultiClassModel updateModel(LogRegressionMultiClassModel newMdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        List<Double> classes = extractClassLabels(datasetBuilder, lbExtractor);

        if(classes.isEmpty())
            return getLastTrainedModelOrThrowEmptyDatasetException(newMdl);

        LogRegressionMultiClassModel multiClsMdl = new LogRegressionMultiClassModel();

        classes.forEach(clsLb -> {
            LogisticRegressionSGDTrainer<?> trainer =
                new LogisticRegressionSGDTrainer<>()
                    .withBatchSize(batchSize)
                    .withLocIterations(amountOfLocIterations)
                    .withMaxIterations(amountOfIterations)
                    .withSeed(seed);

            IgniteBiFunction<K, V, Double> lbTransformer = (k, v) -> {
                Double lb = lbExtractor.apply(k, v);

                if (lb.equals(clsLb))
                    return 1.0;
                else
                    return 0.0;
            };

            LogisticRegressionModel mdl = Optional.ofNullable(newMdl)
                .flatMap(multiClassModel -> multiClassModel.getModel(clsLb))
                .map(learnedModel -> trainer.update(learnedModel, datasetBuilder, featureExtractor, lbTransformer))
                .orElseGet(() -> trainer.fit(datasetBuilder, featureExtractor, lbTransformer));

            multiClsMdl.add(clsLb, mdl);
        });

        return multiClsMdl;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(LogRegressionMultiClassModel mdl) {
        return true;
    }

    /** Iterates among dataset and collects class labels. */
    private <K, V> List<Double> extractClassLabels(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Double> lbExtractor) {
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

                for (double lb : lbs)
                    locClsLabels.add(lb);

                return locClsLabels;
            }, (a, b) -> {
                if (a == null)
                    return b == null ? new HashSet<>() : b;
                if (b == null)
                    return a;
                return Stream.of(a, b).flatMap(Collection::stream).collect(Collectors.toSet());
            });

            if (clsLabels != null)
                res.addAll(clsLabels);

        }
        catch (Exception e) {
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
     * Get the batch size.
     *
     * @return The parameter value.
     */
    public double getBatchSize() {
        return batchSize;
    }

    /**
     * Get the amount of outer iterations of SGD algorithm.
     *
     * @return The parameter value.
     */
    public int getAmountOfIterations() {
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
     * Get the amount of local iterations.
     *
     * @return The parameter value.
     */
    public int getAmountOfLocIterations() {
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
     * Set up the random seed parameter.
     *
     * @param seed Seed for random generator.
     * @return Trainer with new seed parameter value.
     */
    public LogRegressionMultiClassTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * Get the seed for random generator.
     *
     * @return The parameter value.
     */
    public long seed() {
        return seed;
    }

    /**
     * Set up the updates strategy.
     *
     * @param updatesStgy Update strategy.
     * @return Trainer with new update strategy parameter value.
     */
    public LogRegressionMultiClassTrainer withUpdatesStgy(UpdatesStrategy updatesStgy) {
        this.updatesStgy = updatesStgy;
        return this;
    }

    /**
     * Get the update strategy.
     *
     * @return The parameter value.
     */
    public UpdatesStrategy getUpdatesStgy() {
        return updatesStgy;
    }
}
