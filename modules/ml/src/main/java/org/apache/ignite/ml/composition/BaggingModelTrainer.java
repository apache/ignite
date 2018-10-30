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

package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.TrainerTransformers;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Abstract trainer implementing bagging logic. In each learning iteration the algorithm trains one model on subset of
 * learning sample and subspace of features space. Each model is produced from same model-class [e.g. Decision Trees].
 */
public abstract class BaggingModelTrainer<M extends Model<Vector, Double>, R, X> extends DatasetTrainer<ModelsComposition, Double> {
    /**
     * Predictions aggregator.
     */
    private final PredictionsAggregator predictionsAggregator;
    /**
     * Number of features to draw from original features vector to train each model.
     */
    private final int maximumFeaturesCntPerMdl;
    /**
     * Ensemble size.
     */
    private final int ensembleSize;
    /**
     * Size of sample part in percent to train one model.
     */
    private final double samplePartSizePerMdl;
    /**
     * Feature vector size.
     */
    private final int featureVectorSize;

    /**
     * Constructs new instance of BaggingModelTrainer.
     *
     * @param predictionsAggregator    Predictions aggregator.
     * @param featureVectorSize        Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize             Ensemble size.
     * @param samplePartSizePerMdl     Size of sample part in percent to train one model.
     */
    public BaggingModelTrainer(PredictionsAggregator predictionsAggregator,
                               int featureVectorSize,
                               int maximumFeaturesCntPerMdl,
                               int ensembleSize,
                               double samplePartSizePerMdl) {

        this.predictionsAggregator = predictionsAggregator;
        this.maximumFeaturesCntPerMdl = maximumFeaturesCntPerMdl;
        this.ensembleSize = ensembleSize;
        this.samplePartSizePerMdl = samplePartSizePerMdl;
        this.featureVectorSize = featureVectorSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
                                        IgniteBiFunction<K, V, Vector> featureExtractor,
                                        IgniteBiFunction<K, V, Double> lbExtractor) {

        MLLogger log = environment.logger(getClass());
        log.log(MLLogger.VerboseLevel.LOW, "Start learning");

        Long startTs = System.currentTimeMillis();

        try (Dataset<EmptyContext, BootstrappedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BootstrappedDatasetBuilder<>(featureExtractor,
                lbExtractor, ensembleSize, samplePartSizePerMdl)
        )) {
            double learningTime = (double) (System.currentTimeMillis() - startTs) / 1000.0;
            log.log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs", learningTime);
            log.log(MLLogger.VerboseLevel.LOW, "Learning finished");

            return trainEnsemble(dataset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Initialize a single model from the ensemble.
     *
     * @return Initial state of a single model in the ensemble.
     */
    protected abstract M init();

    /**
     * Method representing one local iteration of a single model.
     *
     * @param partitionIdx Index of partition.
     * @param part         Partition.
     * @param modelIdx     Index of trained model.
     * @param projector    Projector on subspace on which to train given model.
     * @param meta         Metadata used for training.
     * @return Result of one local iteration of training of model.
     */
    protected abstract R trainingIteration(
        int partitionIdx,
        BootstrappedDatasetPartition part,
        int modelIdx,
        IgniteFunction<Vector, Vector> projector,
        X meta);

    /**
     * Method used to reduce training results.
     *
     * @param res1 First result.
     * @param res2 Second result.
     * @return Result of reduction.
     */
    protected abstract R reduceTrainingResults(R res1, R res2);

    /**
     * Identity for binary operator reducing training results.
     *
     * @return Identity for binary operator reducing training results.
     */
    protected abstract R identity();

    /**
     * Method describing how training results should be applied to models.
     *
     * @param model Model.
     * @param res   Training result.
     * @return Updated model.
     */
    protected abstract M applyTrainingResultsToModel(M model, R res);

    /**
     * Create metadata which is used during one local iteration.
     *
     * @param models Trained models.
     * @return Metadata which is used during one local iteration.
     */
    protected abstract X getMeta(List<M> models);

    /**
     * Criterion for stopping global iterations.
     *
     * @param iterationsCompleted Number of iterations completed.
     * @param models              Models.
     * @param meta                Metadata.
     * @return Should global iterations loop should stop.
     */
    protected abstract boolean shouldStop(int iterationsCompleted, List<M> models, X meta);

    /**
     * Train ensemble on dataset.
     *
     * @param ds Dataset.
     * @return Models composition trained on given dataset.
     */
    private final ModelsComposition trainEnsemble(Dataset<EmptyContext, BootstrappedDatasetPartition> ds) {
        List<M> models = IntStream.range(0, ensembleSize).mapToObj(i -> init()).collect(Collectors.toList());
        X meta = getMeta(models);
        int iter = 0;
        List<IgniteFunction<Vector, Vector>> projectors = getProjectors();

        while (!shouldStop(iter, models, meta)) {
            List<R> identities = IntStream.range(0, ensembleSize).mapToObj(i -> identity()).collect(Collectors.toList());
            List<R> trainingResults = ds.compute((data, partIdx) -> IntStream
                .range(0, ensembleSize)
                .mapToObj(modelIdx -> trainingIteration(partIdx, data, modelIdx, projectors.get(modelIdx), meta)
                )
                .collect(Collectors.toList()), (l1, l2) -> zipWith(l1, l2, this::reduceTrainingResults), identities);

            models = zipWith(models, trainingResults, this::applyTrainingResultsToModel);
            iter++;
        }

        return new ModelsComposition(models, predictionsAggregator);
    }

    private List<IgniteFunction<Vector, Vector>> getProjectors() {
        // By default all projectors are identities.
        List<IgniteFunction<Vector, Vector>> projectors = IntStream
            .range(0, ensembleSize)
            .mapToObj(mdlIdx -> (IgniteFunction<Vector, Vector>) vector -> vector)
            .collect(Collectors.toList());

        if (maximumFeaturesCntPerMdl > 0) {
            projectors = IntStream
                .range(0, ensembleSize)
                .mapToObj(mdlIdx -> TrainerTransformers.getProjector(TrainerTransformers.getMapping(featureVectorSize, maximumFeaturesCntPerMdl)))
                .collect(Collectors.toList());
        }
        return projectors;
    }

    /**
     * Learn new models on dataset and create new Compositions over them and already learned models.
     *
     * @param mdl              Learned model.
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @param <K>              Type of a key in {@code upstream} data.
     * @param <V>              Type of a value in {@code upstream} data.
     * @return New models composition.
     */
    @Override
    public <K, V> ModelsComposition updateModel(ModelsComposition mdl, DatasetBuilder<K, V> datasetBuilder,
                                                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        ArrayList<Model<Vector, Double>> newModels = new ArrayList<>(mdl.getModels());
        newModels.addAll(fit(datasetBuilder, featureExtractor, lbExtractor).getModels());

        return new ModelsComposition(newModels, predictionsAggregator);
    }

    /**
     * Creates the list which contains results of application of a given bi-function to entries on same positions
     * on given lists. Resulting list size is size of shortest list.
     *
     * @param l1  First list.
     * @param l2  Second list.
     * @param f   Bi-function to apply.
     * @param <X> Type of entries of the first list.
     * @param <Y> Type of entries of the second list.
     * @param <Z> Type of entries of resulting list.
     * @return List which contains results of application of a given bi-function to entries on same positions
     * on given lists.
     */
    private static <X, Y, Z> List<Z> zipWith(List<X> l1, List<Y> l2, IgniteBiFunction<X, Y, Z> f) {
        int lim = Math.min(l1.size(), l2.size());
        List<Z> res = new LinkedList<>();

        for (int i = 0; i < lim; i++) {
            res.add(f.apply(l1.get(i), l2.get(i)));
        }

        return res;
    }
}
