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
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.combinators.parallel.TrainersParallelComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.AdaptableDatasetTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.transformers.BaggingUpstreamTransformer;
import org.apache.ignite.ml.util.Utils;

/**
 * Trainer encapsulating logic of bootstrap aggregating (bagging).
 * This trainer accepts some other trainer and returns bagged version of it.
 * Resulting model consists of submodels results of which are aggregated by a specified aggregator.
 * <p>Bagging is done
 * on both samples and features (<a href="https://en.wikipedia.org/wiki/Bootstrap_aggregating"></a>Samples bagging</a>,
 * <a href="https://en.wikipedia.org/wiki/Random_subspace_method"></a>Features bagging</a>).</p>
 *
 * @param <L> Type of labels.
 */
public class BaggedTrainer<L> extends
    DatasetTrainer<BaggedModel, L> {
    /** Trainer for which bagged version is created. */
    private final DatasetTrainer<? extends IgniteModel, L> tr;

    /** Aggregator of submodels results. */
    private final PredictionsAggregator aggregator;

    /** Count of submodels in the ensemble. */
    private final int ensembleSize;

    /** Ratio determining which part of dataset will be taken as subsample for each submodel training. */
    private final double subsampleRatio;

    /** Dimensionality of feature vectors. */
    private final int featuresVectorSize;

    /** Dimension of subspace on which all samples from subsample are projected. */
    private final int featureSubspaceDim;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param tr Trainer for making bagged.
     * @param aggregator Aggregator of models.
     * @param ensembleSize Size of ensemble.
     * @param subsampleRatio Ratio (subsample size) / (initial dataset size).
     * @param featuresVectorSize Dimensionality of feature vector.
     * @param featureSubspaceDim Dimensionality of feature subspace.
     */
    public BaggedTrainer(DatasetTrainer<? extends IgniteModel, L> tr,
        PredictionsAggregator aggregator, int ensembleSize, double subsampleRatio, int featuresVectorSize,
        int featureSubspaceDim) {
        this.tr = tr;
        this.aggregator = aggregator;
        this.ensembleSize = ensembleSize;
        this.subsampleRatio = subsampleRatio;
        this.featuresVectorSize = featuresVectorSize;
        this.featureSubspaceDim = featureSubspaceDim;
    }

    /**
     * Create trainer bagged trainer.
     *
     * @return Bagged trainer.
     */
    private DatasetTrainer<IgniteModel<Vector, Double>, L> getTrainer() {
        List<int[]> mappings = (featuresVectorSize > 0 && featureSubspaceDim != featuresVectorSize) ?
            IntStream.range(0, ensembleSize).mapToObj(
                modelIdx -> getMapping(
                    featuresVectorSize,
                    featureSubspaceDim,
                    environment.randomNumbersGenerator().nextLong()))
                .collect(Collectors.toList()) :
            null;

        List<DatasetTrainer<? extends IgniteModel, L>> trainers = Collections.nCopies(ensembleSize, tr);

        // Generate a list of trainers each each copy of original trainer but on its own subspace and subsample.
        List<DatasetTrainer<IgniteModel<Vector, Double>, L>> subspaceTrainers = IntStream.range(0, ensembleSize)
            .mapToObj(mdlIdx -> {
                AdaptableDatasetTrainer<Vector, Double, Vector, Double, ? extends IgniteModel, L> tr =
                    AdaptableDatasetTrainer.of(trainers.get(mdlIdx));
                if (mappings != null) {
                    tr = tr.afterFeatureExtractor(featureValues -> {
                        int[] mapping = mappings.get(mdlIdx);
                        double[] newFeaturesValues = new double[mapping.length];
                        for (int j = 0; j < mapping.length; j++)
                            newFeaturesValues[j] = featureValues.get(mapping[j]);

                        return VectorUtils.of(newFeaturesValues);
                    }).beforeTrainedModel(VectorUtils.getProjector(mappings.get(mdlIdx)));
                }
                return tr
                    .withUpstreamTransformerBuilder(BaggingUpstreamTransformer.builder(subsampleRatio, mdlIdx))
                    .withEnvironmentBuilder(envBuilder);
            })
            .map(CompositionUtils::unsafeCoerce)
            .collect(Collectors.toList());

        AdaptableDatasetTrainer<Vector, Double, Vector, List<Double>, IgniteModel<Vector, List<Double>>, L> finalTrainer = AdaptableDatasetTrainer.of(
            new TrainersParallelComposition<>(
                subspaceTrainers)).afterTrainedModel(l -> aggregator.apply(l.stream().mapToDouble(Double::valueOf).toArray()));

        return CompositionUtils.unsafeCoerce(finalTrainer);
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



    /** {@inheritDoc} */
    @Override public <K, V> BaggedModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        IgniteModel<Vector, Double> fit = getTrainer().fit(datasetBuilder, preprocessor);
        return new BaggedModel(fit);
    }

    /** {@inheritDoc} */
    @Override public <K, V> BaggedModel update(BaggedModel mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        learningEnvironment().initDeployingContext(preprocessor);

        IgniteModel<Vector, Double> updated = getTrainer().update(mdl.model(), datasetBuilder, preprocessor);
        return new BaggedModel(updated);
    }

    /** {@inheritDoc} */
    @Override public BaggedTrainer<L> withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (BaggedTrainer<L>)super.withEnvironmentBuilder(envBuilder);
    }

    /**
     * This method is never called, instead of constructing logic of update from
     * {@link DatasetTrainer#updateModel}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return True if current critical for training parameters correspond to parameters from last training.
     */
    @Override public boolean isUpdateable(BaggedModel mdl) {
        // Should be never called.
        throw new IllegalStateException();
    }

    /**
     * This method is never called, instead of constructing logic of update from
     * {@link DatasetTrainer#isUpdateable} and
     * {@link DatasetTrainer#updateModel}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return Updated model.
     */
    @Override protected <K, V> BaggedModel updateModel(BaggedModel mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        // Should be never called.
        throw new IllegalStateException();
    }
}
