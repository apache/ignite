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

package org.apache.ignite.ml.composition.stacking;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * {@link DatasetTrainer} encapsulating stacking technique for model training.
 * Model produced by this trainer consists of two layers. First layer is a model {@code IS -> IA}.
 * This layer is a "parallel" composition of several "submodels", each of them itself is a model
 * {@code IS -> IA} with their outputs {@code [IA]} merged into single {@code IA}.
 * Second layer is an aggregator model {@code IA -> O}.
 * Training corresponds to this layered structure in the following way:
 * <pre>
 * 1. train models of first layer;
 * 2. train aggregator model on dataset augmented with outputs of first layer models converted to vectors.
 * </pre>
 * During second step we can choose if we want to keep original features along with converted outputs of first layer
 * models or use only converted results of first layer models. This choice will also affect inference.
 * This class is a most general stacked trainer, there is a {@link StackedVectorDatasetTrainer}: a shortcut version of
 * it with some types and functions specified.
 *
 * @param <IS> Type of submodels input.
 * @param <IA> Type of aggregator input.
 * @param <O> Type of aggregator output.
 * @param <L> Type of labels.
 */
public class StackedDatasetTrainer<IS, IA, O, AM extends IgniteModel<IA, O>, L>
    extends DatasetTrainer<StackedModel<IS, IA, O, AM>, L> {
    /** Operator that merges inputs for aggregating model. */
    private IgniteBinaryOperator<IA> aggregatingInputMerger;

    /** Function transforming input for submodels to input for aggregating model. */
    private IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter;

    /** Trainers of submodels with converters from and to {@link Vector}. */
    private List<DatasetTrainer<IgniteModel<IS, IA>, L>> submodelsTrainers;

    /** Aggregating trainer. */
    private DatasetTrainer<AM, L> aggregatorTrainer;

    /** Function used for conversion of {@link Vector} to submodel input. */
    private IgniteFunction<Vector, IS> vector2SubmodelInputConverter;

    /** Function used for conversion of submodel output to {@link Vector}. */
    private IgniteFunction<IA, Vector> submodelOutput2VectorConverter;

    /**
     * Create instance of this class.
     *
     * @param aggregatorTrainer Trainer of model used for aggregation of results of submodels.
     * @param aggregatingInputMerger Binary operator used to merge outputs of submodels into one output passed to
     * aggregator model.
     * @param submodelInput2AggregatingInputConverter Function used to convert input of submodel to output of submodel
     * this function is used if user chooses to keep original features.
     * @param submodelsTrainers List of submodel trainers.
     */
    public StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatorTrainer,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter,
        List<DatasetTrainer<IgniteModel<IS, IA>, L>> submodelsTrainers,
        IgniteFunction<Vector, IS> vector2SubmodelInputConverter,
        IgniteFunction<IA, Vector> submodelOutput2VectorConverter) {
        this.aggregatorTrainer = aggregatorTrainer;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.submodelInput2AggregatingInputConverter = submodelInput2AggregatingInputConverter;
        this.submodelsTrainers = new ArrayList<>(submodelsTrainers);
        this.vector2SubmodelInputConverter = vector2SubmodelInputConverter;
        this.submodelOutput2VectorConverter = submodelOutput2VectorConverter;
    }

    /**
     * Constructs instance of this class.
     *
     * @param aggregatorTrainer Trainer of model used for aggregation of results of submodels.
     * @param aggregatingInputMerger Binary operator used to merge outputs of submodels into one output passed to
     * aggregator model.
     * @param submodelInput2AggregatingInputConverter Function used to convert input of submodel to output of submodel
     * this function is used if user chooses to keep original features.
     */
    public StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatorTrainer,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter) {
        this(aggregatorTrainer,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter,
            new ArrayList<>(),
            null,
            null);
    }

    /**
     * Constructs instance of this class.
     */
    public StackedDatasetTrainer() {
        this(null, null, null, new ArrayList<>(), null, null);
    }

    /**
     * Keep original features during training and propagate submodels input to aggregator during inference
     * using given function.
     * Note that if this object is on, training will be done on vector obtaining from
     * concatenating features passed to submodels trainers and outputs of submodels converted to vectors, this can,
     * for example influence aggregator model input vector dimension (if {@code IS = Vector}), or, more generally,
     * some {@code IS} parameters which are not reflected just by its type. So converter should be
     * written accordingly.
     *
     * @param submodelInput2AggregatingInputConverter Function used to propagate submodels input to aggregator.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withOriginalFeaturesKept(
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter) {
        this.submodelInput2AggregatingInputConverter = submodelInput2AggregatingInputConverter;

        return this;
    }

    /**
     * Drop original features during training and inference.
     *
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withOriginalFeaturesDropped() {
        submodelInput2AggregatingInputConverter = null;

        return this;
    }

    /**
     * Set function used for conversion of submodel output to {@link Vector}. This function is used during
     * building of dataset for training aggregator model. This dataset is augmented with results of submodels
     * converted to {@link Vector}.
     *
     * @param submodelOutput2VectorConverter Function used for conversion of submodel output to {@link Vector}.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withSubmodelOutput2VectorConverter(
        IgniteFunction<IA, Vector> submodelOutput2VectorConverter) {
        this.submodelOutput2VectorConverter = submodelOutput2VectorConverter;

        return this;
    }

    /**
     * Set function used for conversion of {@link Vector} to submodel input. This function is used during
     * building of dataset for training aggregator model. This dataset is augmented with results of submodels
     * applied to {@link Vector}s in original dataset.
     *
     * @param vector2SubmodelInputConverter Function used for conversion of {@link Vector} to submodel input.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withVector2SubmodelInputConverter(
        IgniteFunction<Vector, IS> vector2SubmodelInputConverter) {
        this.vector2SubmodelInputConverter = vector2SubmodelInputConverter;

        return this;
    }

    /**
     * Specify aggregator trainer.
     *
     * @param aggregatorTrainer Aggregator trainer.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withAggregatorTrainer(DatasetTrainer<AM, L> aggregatorTrainer) {
        this.aggregatorTrainer = aggregatorTrainer;

        return this;
    }

    /**
     * Specify binary operator used to merge submodels outputs to one.
     *
     * @param merger Binary operator used to merge submodels outputs to one.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, AM, L> withAggregatorInputMerger(IgniteBinaryOperator<IA> merger) {
        aggregatingInputMerger = merger;

        return this;
    }

    /**
     * Adds submodel trainer along with converters needed on training and inference stages.
     *
     * @param trainer Submodel trainer.
     * @return This object.
     */
    @SuppressWarnings({"unchecked"})
    public <M1 extends IgniteModel<IS, IA>> StackedDatasetTrainer<IS, IA, O, AM, L> addTrainer(
        DatasetTrainer<M1, L> trainer) {
        // Unsafely coerce DatasetTrainer<M1, L> to DatasetTrainer<Model<IS, IA>, L>, but we fully control
        // usages of this unsafely coerced object, on the other hand this makes work with
        // submodelTrainers easier.
        submodelsTrainers.add(new DatasetTrainer<IgniteModel<IS, IA>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V> IgniteModel<IS, IA> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return trainer.fit(datasetBuilder, featureExtractor, lbExtractor);
            }

            /** {@inheritDoc} */
            @Override public <K, V> IgniteModel<IS, IA> update(IgniteModel<IS, IA> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                DatasetTrainer<IgniteModel<IS, IA>, L> trainer1 = (DatasetTrainer<IgniteModel<IS, IA>, L>)trainer;
                return trainer1.update(mdl, datasetBuilder, featureExtractor, lbExtractor);
            }

            /** {@inheritDoc} */
            @Override protected boolean checkState(IgniteModel<IS, IA> mdl) {
                return true;
            }

            /** {@inheritDoc} */
            @Override protected <K, V> IgniteModel<IS, IA> updateModel(IgniteModel<IS, IA> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return null;
            }
        });

        return this;
    }

    /** {@inheritDoc} */
    @Override public <K, V> StackedModel<IS, IA, O, AM> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {

        return update(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override public <K, V> StackedModel<IS, IA, O, AM> update(StackedModel<IS, IA, O, AM> mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return runOnSubmodels(
            ensemble -> {
                List<IgniteSupplier<IgniteModel<IS, IA>>> res = new ArrayList<>();
                for (int i = 0; i < ensemble.size(); i++) {
                    final int j = i;
                    res.add(() -> {
                        DatasetTrainer<IgniteModel<IS, IA>, L> trainer = ensemble.get(j);
                        return mdl == null ?
                            trainer.fit(datasetBuilder, featureExtractor, lbExtractor) :
                            trainer.update(mdl.submodels().get(j), datasetBuilder, featureExtractor, lbExtractor);
                    });
                }
                return res;
            },
            (at, extr) -> mdl == null ?
                at.fit(datasetBuilder, extr, lbExtractor) :
                at.update(mdl.aggregatorModel(), datasetBuilder, extr, lbExtractor),
            featureExtractor
        );
    }

    /** {@inheritDoc} */
    @Override public StackedDatasetTrainer<IS, IA, O, AM, L> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        submodelsTrainers =
            submodelsTrainers.stream().map(x -> x.withEnvironmentBuilder(envBuilder)).collect(Collectors.toList());
        aggregatorTrainer = aggregatorTrainer.withEnvironmentBuilder(envBuilder);

        return this;
    }

    /**
     * <pre>
     * 1. Obtain models produced by running specified tasks;
     * 2. run other specified task on dataset augmented with results of models from step 2.
     * </pre>
     *
     * @param taskSupplier Function used to generate tasks for first step.
     * @param aggregatorProcessor Function used
     * @param featureExtractor Feature extractor.
     * @param <K> Type of keys in upstream.
     * @param <V> Type of values in upstream.
     * @return {@link StackedModel}.
     */
    private <K, V> StackedModel<IS, IA, O, AM> runOnSubmodels(
        IgniteFunction<List<DatasetTrainer<IgniteModel<IS, IA>, L>>, List<IgniteSupplier<IgniteModel<IS, IA>>>> taskSupplier,
        IgniteBiFunction<DatasetTrainer<AM, L>, IgniteBiFunction<K, V, Vector>, AM> aggregatorProcessor,
        IgniteBiFunction<K, V, Vector> featureExtractor) {

        // Make sure there is at least one way for submodel input to propagate to aggregator.
        if (submodelInput2AggregatingInputConverter == null && submodelsTrainers.isEmpty())
            throw new IllegalStateException("There should be at least one way for submodels " +
                "input to be propageted to aggregator.");

        if (submodelOutput2VectorConverter == null || vector2SubmodelInputConverter == null)
            throw new IllegalStateException("There should be a specified way to convert vectors to submodels " +
                "input and submodels output to vector");

        if (aggregatingInputMerger == null)
            throw new IllegalStateException("Binary operator used to convert outputs of submodels is not specified");

        List<IgniteSupplier<IgniteModel<IS, IA>>> mdlSuppliers = taskSupplier.apply(submodelsTrainers);

        List<IgniteModel<IS, IA>> subMdls = environment.parallelismStrategy().submit(mdlSuppliers).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        // Add new columns consisting in submodels output in features.
        IgniteBiFunction<K, V, Vector> augmentedExtractor = getFeatureExtractorForAggregator(featureExtractor,
            subMdls,
            submodelInput2AggregatingInputConverter,
            submodelOutput2VectorConverter,
            vector2SubmodelInputConverter);

        AM aggregator = aggregatorProcessor.apply(aggregatorTrainer, augmentedExtractor);

        StackedModel<IS, IA, O, AM> res = new StackedModel<>(
            aggregator,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter);

        for (IgniteModel<IS, IA> subMdl : subMdls)
            res.addSubmodel(subMdl);

        return res;
    }

    /**
     * Get feature extractor which will be used for aggregator trainer from original feature extractor.
     * This method is static to make sure that we will not grab context of instance in serialization.
     *
     * @param featureExtractor Original feature extractor.
     * @param subMdls Submodels.
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Feature extractor which will be used for aggregator trainer from original feature extractor.
     */
    private static <IS, IA, K, V> IgniteBiFunction<K, V, Vector> getFeatureExtractorForAggregator(
        IgniteBiFunction<K, V, Vector> featureExtractor, List<IgniteModel<IS, IA>> subMdls,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter,
        IgniteFunction<IA, Vector> submodelOutput2VectorConverter,
        IgniteFunction<Vector, IS> vector2SubmodelInputConverter) {
        if (submodelInput2AggregatingInputConverter != null)
            return featureExtractor.andThen((Vector v) -> {
                Vector[] vs = subMdls.stream().map(sm ->
                    applyToVector(sm, submodelOutput2VectorConverter, vector2SubmodelInputConverter, v)).toArray(Vector[]::new);
                return VectorUtils.concat(v, vs);
            });
        else
            return featureExtractor.andThen((Vector v) -> {
                Vector[] vs = subMdls.stream().map(sm ->
                    applyToVector(sm, submodelOutput2VectorConverter, vector2SubmodelInputConverter, v)).toArray(Vector[]::new);
                return VectorUtils.concat(vs);
            });
    }

    /**
     * Apply submodel to {@link Vector}.
     *
     * @param mdl Submodel.
     * @param submodelOutput2VectorConverter Function for conversion of submodel output to {@link Vector}.
     * @param vector2SubmodelInputConverter Function used for conversion of {@link Vector} to submodel input.
     * @param v Vector.
     * @param <IS> Type of submodel input.
     * @param <IA> Type of submodel output.
     * @return Result of application of {@code submodelOutput2VectorConverter . mdl . vector2SubmodelInputConverter}
     * where dot denotes functions composition.
     */
    private static <IS, IA> Vector applyToVector(IgniteModel<IS, IA> mdl,
        IgniteFunction<IA, Vector> submodelOutput2VectorConverter,
        IgniteFunction<Vector, IS> vector2SubmodelInputConverter,
        Vector v) {
        return vector2SubmodelInputConverter.andThen(mdl::predict).andThen(submodelOutput2VectorConverter).apply(v);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> StackedModel<IS, IA, O, AM> updateModel(StackedModel<IS, IA, O, AM> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        // This method is never called, we override "update" instead.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(StackedModel<IS, IA, O, AM> mdl) {
        return true;
    }
}
