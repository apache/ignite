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

package org.apache.ignite.ml.trainers.transformers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.Model;
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
 *
 * @param <IS> Type of submodels input.
 * @param <IA> Type of aggregator input.
 * @param <O> Type of aggregator output.
 * @param <L> Type of labels.
 * @param <AM> Type of aggregator model.
 */
public class StackedDatasetTrainer<IS, IA, O, L, AM extends Model<IA, O>>
    extends DatasetTrainer<StackedModel<IS, IA, O, AM>, L> {
    /** Operator tht merges inputs for aggregating model. */
    private IgniteBinaryOperator<IA> aggregatingInputMerger;

    /** Function transforming input for submodels to input for aggregating model. */
    private IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter;

    /** Trainers of submodels with converters from and to {@link Vector}. */
    public List<TrainerWithConverters<IA, IS, ?, L, AM>> submodelsTrainers;

    /** Aggregating trainer. */
    public DatasetTrainer<AM, L> aggregatorTrainer;

    protected StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatorTrainer,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter,
        List<TrainerWithConverters<IA, IS, ?, L, AM>> submodelsTrainers) {
        this.aggregatorTrainer = aggregatorTrainer;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.submodelInput2AggregatingInputConverter = submodelInput2AggregatingInputConverter;
        this.submodelsTrainers = new ArrayList<>(submodelsTrainers);
    }

    public StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatorTrainer,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter) {
        this(aggregatorTrainer,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter,
            new ArrayList<>());
    }

    /**
     * Constructs instance of this class.
     */
    public StackedDatasetTrainer() {
        this(null,null,null, new ArrayList<>());
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
    public StackedDatasetTrainer<IS, IA, O, L, AM> keepingOriginalFeatures(
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter) {
        this.submodelInput2AggregatingInputConverter = submodelInput2AggregatingInputConverter;

        return this;
    }

    /**
     * Drop original features during training and inference.
     *
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, L, AM> droppingOriginalFeatures() {
        this.submodelInput2AggregatingInputConverter = null;

        return this;
    }

    /**
     * Specify aggregator trainer.
     *
     * @param aggregatorTrainer Aggregator trainer.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, L, AM> withAggregatorTrainer(DatasetTrainer<AM, L> aggregatorTrainer) {
        this.aggregatorTrainer = aggregatorTrainer;

        return this;
    }

    /**
     * Specify binary operator used to merge submodels outputs to one.
     *
     * @param merger Binary operator used to merge submodels outputs to one.
     * @return This object.
     */
    public StackedDatasetTrainer<IS, IA, O, L, AM> withAggregatorInputMerger(IgniteBinaryOperator<IA> merger) {
        this.aggregatingInputMerger = merger;

        return this;
    }

    /**
     * Adds submodel trainer along with converters needed on training and inference stages.
     *
     * @param trainer Sumbodel trainer.
     * @param vec2InputConverter Function used to convert {@link Vector} to input for submodel produced by trainer.
     * Used during training.
     * @param output2VecConverter Function used to convert output of submodel produced by trainer to {@link Vector}.
     * Used during training.
     * @param submodelOutput2AggregatingInputConverter Function used to convert output of submodel produced by
     * trainer to input of aggregator. Used during inference.
     * @param <O1> Type of output of submodel produced by trainer.
     * @return This object.
     */
    public <O1> StackedDatasetTrainer<IS, IA, O, L, AM> withAddedTrainer(DatasetTrainer<Model<IS, O1>, L> trainer,
        IgniteFunction<Vector, IS> vec2InputConverter,
        IgniteFunction<O1, Vector> output2VecConverter,
        IgniteFunction<O1, IA> submodelOutput2AggregatingInputConverter) {
        submodelsTrainers.add(new TrainerWithConverters<>(trainer,
            vec2InputConverter,
            output2VecConverter,
            submodelOutput2AggregatingInputConverter));

        return this;
    }

    /** {@inheritDoc} */
    @Override public <K, V> StackedModel<IS, IA, O, AM> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return runOnSubmodels(
            ensemble ->
                ensemble.stream()
                    .map(tc -> (IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>)(() -> tc.fit(datasetBuilder, featureExtractor, lbExtractor)))
                    .collect(Collectors.toList()),
            (at, extr) -> at.fit(datasetBuilder, extr, lbExtractor),
            featureExtractor
        );
    }

    /** {@inheritDoc} */
    @Override protected <K, V> StackedModel<IS, IA, O, AM> updateModel(StackedModel<IS, IA, O, AM> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return runOnSubmodels(
            ensemble -> {
                int i = 0;
                List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>> res = new ArrayList<>();
                for (Model<IS, ?> submodel : mdl.submodels()) {
                    // Trick to pass 'i' into lambda.
                    int j = i;
                    res.add(() -> ensemble.get(j).updateModel(submodel, datasetBuilder, featureExtractor, lbExtractor));
                    i++;
                }
                return res;
            },
            (at, extr) -> at.fit(datasetBuilder, extr, lbExtractor),
            featureExtractor
        );
    }

    /** {@inheritDoc} */
    @Override public boolean checkState(StackedModel<IS, IA, O, AM> mdl) {
        boolean res = true;
        int i = 0;
        for (Model<IS, ?> submodel : mdl.submodels()) {
            res &= submodelsTrainers.get(i).checkState(submodel);
            i++;
        }

        return res && aggregatorTrainer.checkState(mdl.aggregatingModel());
    }

    /** {@inheritDoc} */
    @Override public StackedDatasetTrainer<IS, IA, O, L, AM> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        submodelsTrainers =
            submodelsTrainers.stream().map(x -> x.withEnvironmentBuilder(envBuilder)).collect(Collectors.toList());
        aggregatorTrainer = aggregatorTrainer.withEnvironmentBuilder(envBuilder);

        return this;
    }

    protected <K, V> StackedModel<IS, IA, O, AM> runOnSubmodels(
        IgniteFunction<List<TrainerWithConverters<IA, IS, ?, L, AM>>, List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>>> taskSupplier,
        IgniteBiFunction<DatasetTrainer<AM, L>, IgniteBiFunction<K, V, Vector>, AM> aggregatorProcessor,
        IgniteBiFunction<K, V, Vector> featureExtractor) {

        // Make sure there is at least one way for submodel input to propagate to aggregator.
        assert submodelInput2AggregatingInputConverter != null || !submodelsTrainers.isEmpty();

        List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>> mdlSuppliers = taskSupplier.apply(submodelsTrainers);

        List<ModelWithConverters<IA, IS, ?, AM>> subMdls = environment.parallelismStrategy().submit(mdlSuppliers).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        // Add new columns consisting in submodels output in features.
        IgniteBiFunction<K, V, Vector> augmentedExtractor = getFeatureExtractorForAggregator(featureExtractor, subMdls);

        AM aggregator = aggregatorProcessor.apply(aggregatorTrainer, augmentedExtractor);

        StackedModel<IS, IA, O, AM> res = new StackedModel<>(
            aggregator,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter);

        for (ModelWithConverters<IA, IS, ?, AM> subMdl : subMdls)
            subMdl.addToStackedModel(res);

        return res;
    }

    private <K, V> IgniteBiFunction<K, V, Vector> getFeatureExtractorForAggregator(
        IgniteBiFunction<K, V, Vector> featureExtractor, List<ModelWithConverters<IA, IS, ?, AM>> subMdls) {
        if (submodelInput2AggregatingInputConverter != null)
            return featureExtractor.andThen((Vector v) -> {
                Vector[] objects = subMdls.stream().map(sm -> sm.applyToVector(v)).toArray(Vector[]::new);
                return VectorUtils.concat(v, objects);
            });
        else
            return featureExtractor.andThen((Vector v) -> {
                Vector[] objects = subMdls.stream().map(sm -> sm.applyToVector(v)).toArray(Vector[]::new);
                return VectorUtils.concat(objects);
            });

    }

    private static class SomethingWithConverters<IA, I, O, T> implements Serializable  {
        /** */
        private static final long serialVersionUID = -5017645720067015574L;

        T val;
        IgniteFunction<Vector, I> inputConverter;
        IgniteFunction<O, Vector> outputConverter;
        IgniteFunction<O, IA> outputToAggregatingInputConverter;

        public SomethingWithConverters(
            T val,
            IgniteFunction<Vector, I> inputConverter,
            IgniteFunction<O, Vector> outputConverter,
            IgniteFunction<O, IA> outputToAggregatingInputConverter) {
            this.val = val;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
            this.outputToAggregatingInputConverter = outputToAggregatingInputConverter;
        }
    }

    private static class TrainerWithConverters<IA, I, O, L, AM extends Model<IA, ?>> implements Serializable  {
        /** */
        private static final long serialVersionUID = -5017645720067015574L;

        DatasetTrainer<Model<I, O>, L> trainer;
        IgniteFunction<Vector, I> inputConverter;
        IgniteFunction<O, Vector> outputConverter;
        IgniteFunction<O, IA> outputToAggregatingInputConverter;

        public TrainerWithConverters(
            DatasetTrainer<Model<I, O>, L> trainer,
            IgniteFunction<Vector, I> inputConverter,
            IgniteFunction<O, Vector> outputConverter,
            IgniteFunction<O, IA> outputToAggregatingInputConverter) {
            this.trainer = trainer;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
            this.outputToAggregatingInputConverter = outputToAggregatingInputConverter;
        }

        boolean checkState(Model<I, ?> mdl) {
            return trainer.checkState((Model<I, O>)mdl);
        }

        <K, V> ModelWithConverters<IA, I, O, AM> fit(DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featuresExtractor,
            IgniteBiFunction<K, V, L> lbExtractor) {
            Model<I, O> mdl = trainer.fit(datasetBuilder, featuresExtractor, lbExtractor);
            return new ModelWithConverters<>(mdl,
                inputConverter,
                outputConverter,
                outputToAggregatingInputConverter);
        }

        <K, V> ModelWithConverters<IA, I, O, AM> updateModel(Model<I, ?> mdl,
            DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featureExtractor,
            IgniteBiFunction<K, V, L> lbExtractor) {
            Model<I, O> updatedMdl = trainer.update((Model<I, O>)mdl, datasetBuilder, featureExtractor, lbExtractor);
            return new ModelWithConverters<>(updatedMdl,
                inputConverter,
                outputConverter, outputToAggregatingInputConverter);
        }

        TrainerWithConverters<IA, I, O, L, AM> withEnvironmentBuilder(LearningEnvironmentBuilder environmentBuilder) {
            return new TrainerWithConverters<>(trainer.withEnvironmentBuilder(environmentBuilder),
                inputConverter,
                outputConverter,
                outputToAggregatingInputConverter);
        }
    }

    private static class ModelWithConverters<IA, I, O, AM extends Model<IA, ?>> implements Serializable {
        /** */
        private static final long serialVersionUID = -8873722548655893591L;

        Model<I, O> mdl;
        IgniteFunction<Vector, I> inputConverter;
        IgniteFunction<O, Vector> outputConverter;
        IgniteFunction<O, IA> outputToAggregatingInputConverter;

        ModelWithConverters(
            Model<I, O> mdl,
            IgniteFunction<Vector, I> inputConverter,
            IgniteFunction<O, Vector> outputConverter,
            IgniteFunction<O, IA> outputToAggregatingInputConverter) {
            this.mdl = mdl;
            this.inputConverter = inputConverter;
            this.outputConverter = outputConverter;
            this.outputToAggregatingInputConverter = outputToAggregatingInputConverter;
        }

        void addToStackedModel(StackedModel<I, IA, ?, AM> stackedMdl) {
            stackedMdl.addSubmodel(i -> mdl.andThen(outputToAggregatingInputConverter).apply(i));
        }

        Vector applyToVector(Vector v) {
            Model<Vector, Vector> id = Model.identityModel();
            return id.andThen(inputConverter).andThen(mdl).andThen(outputConverter).apply(v);
        }

        Model<I, IA> wrapped() {
            return i -> mdl.andThen(outputToAggregatingInputConverter).apply(i);
        }
    }
}
