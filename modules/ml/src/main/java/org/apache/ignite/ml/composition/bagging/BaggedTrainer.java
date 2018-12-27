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

package org.apache.ignite.ml.composition.bagging;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.combinators.parallel.SameTrainersParallelComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.AdaptableDatasetTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.transformers.BaggingUpstreamTransformer;
import org.apache.ignite.ml.util.Utils;

public class BaggedTrainer<I, M extends Model<I, Double>, L, T extends DatasetTrainer<M, L>> extends
    DatasetTrainer<BaggedModel<I>, L> {
    private final DatasetTrainer<M, L> tr;
    private final PredictionsAggregator aggregator;
    private final int ensembleSize;
    private final double subsampleRatio;
    private final int featuresVectorSize;
    private final int featureSubspaceDim;

    public BaggedTrainer(DatasetTrainer<M, L> tr,
        PredictionsAggregator aggregator, int ensembleSize, double subsampleRatio, int featuresVectorSize,
        int featureSubspaceDim) {
        this.tr = tr;
        this.aggregator = aggregator;
        this.ensembleSize = ensembleSize;
        this.subsampleRatio = subsampleRatio;
        this.featuresVectorSize = featuresVectorSize;
        this.featureSubspaceDim = featureSubspaceDim;
    }

    private DatasetTrainer<Model<I, List<Double>>, L> getTrainer() {
        List<int[]> mappings = (featuresVectorSize > 0 && featureSubspaceDim != featuresVectorSize) ?
            IntStream.range(0, ensembleSize).mapToObj(
                modelIdx -> getMapping(
                    featuresVectorSize,
                    featureSubspaceDim,
                    environment.randomNumbersGenerator().nextLong() + modelIdx))
                .collect(Collectors.toList()) :
            null;

        Stream.generate(this::getTrainer).limit(ensembleSize);
        mappings.stream();
        List<DatasetTrainer<M, L>> trainers = Collections.nCopies(ensembleSize, tr);

        // Generate a list of trainers each each copy of original trainer but on its own subspace and subsample.
        List<DatasetTrainer<Model<I, Double>, L>> subspaceTrainers = IntStream.range(0, ensembleSize)
            .mapToObj(mdlIdx ->
                AdaptableDatasetTrainer.of(trainers.get(mdlIdx))
                    .afterFeatureExtractor(featureValues -> {
                        int[] mapping = mappings.get(mdlIdx);
                        double[] newFeaturesValues = new double[mapping.length];
                        for (int j = 0; j < mapping.length; j++)
                            newFeaturesValues[j] = featureValues.get(mapping[j]);

                        return VectorUtils.of(newFeaturesValues);
                    })
                    .withUpstreamTransformerBuilder(BaggingUpstreamTransformer.builder(subsampleRatio, mdlIdx))
            )
            .map(CompositionUtils::unsafeCoerce)
            .collect(Collectors.toList());

        return new SameTrainersParallelComposition<>(subspaceTrainers);
    }

    /**
     * Get mapping R^featuresVectorSize -> R^maximumFeaturesCntPerMdl.
     *
     * @param featuresVectorSize Features vector size (Dimension of initial space).
     * @param maximumFeaturesCntPerMdl Dimension of target space.
     * @param seed Seed.
     * @return Mapping R^featuresVectorSize -> R^maximumFeaturesCntPerMdl.
     */
    public static int[] getMapping(int featuresVectorSize, int maximumFeaturesCntPerMdl, long seed) {
        return Utils.selectKDistinct(featuresVectorSize, maximumFeaturesCntPerMdl, new Random(seed));
    }

    /**
     * Get projector from index mapping.
     *
     * @param mapping Index mapping.
     * @return Projector.
     */
    public static IgniteFunction<Vector, Vector> getProjector(int[] mapping) {
        return v -> {
            Vector res = VectorUtils.zeroes(mapping.length);
            for (int i = 0; i < mapping.length; i++)
                res.set(i, v.get(mapping[i]));

            return res;
        };
    }

    /** {@inheritDoc} */
    @Override public <K, V> BaggedModel<I> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        Model<I, List<Double>> fit = getTrainer().fit(datasetBuilder, featureExtractor, lbExtractor);
        return new BaggedModel<>(fit, aggregator);
    }

    /** {@inheritDoc} */
    @Override public <K, V> BaggedModel<I> update(BaggedModel<I> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        Model<I, List<Double>> updated = getTrainer().update(mdl.model(), datasetBuilder, featureExtractor, lbExtractor);
        return new BaggedModel<>(updated, aggregator);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(BaggedModel<I> mdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> BaggedModel<I> updateModel(BaggedModel<I> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }
}
