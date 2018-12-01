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
import java.util.stream.Stream;
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

public class StackedDatasetTrainer<IS, IA, O, L, AM extends Model<IA, O>> extends DatasetTrainer<StackedModel<IS, IA, O, AM>, L> {
    private final IgniteBinaryOperator<IA> aggregatingInputMerger;
    private final IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter;
    public List<TrainerWithConverters<IA, IS, ?, L, AM>> ensemble;
    public DatasetTrainer<AM, L> aggregatingTrainer;

    public StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> submodelInput2AggregatingInputConverter) {
        this.aggregatingTrainer = aggregatingTrainer;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.submodelInput2AggregatingInputConverter = submodelInput2AggregatingInputConverter;
        this.ensemble = new ArrayList<>();
    }

    public <O1> StackedDatasetTrainer<IS, IA, O, L, AM> withAddedTrainer(DatasetTrainer<Model<IS, O1>, L> trainer,
        IgniteFunction<Vector, IS> vec2InputConverter,
        IgniteFunction<O1, IA> submodelOutput2AggregatingInputConverter,
        IgniteFunction<O1, Vector> output2VecConverter) {
        ensemble.add(new TrainerWithConverters<>(trainer,
            vec2InputConverter,
            output2VecConverter,
            submodelOutput2AggregatingInputConverter));

        return this;
    }

    @Override public <K, V> StackedModel<IS, IA, O, AM> fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return runOnEnsemble(
            ensemble ->
                ensemble.stream()
                    .map(tc -> (IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>)(() -> tc.fit(datasetBuilder, featureExtractor, lbExtractor)))
                    .collect(Collectors.toList()),
            (at, extr) -> at.fit(datasetBuilder, extr, lbExtractor),
            featureExtractor
        );
    }

    @Override protected <K, V> StackedModel<IS, IA, O, AM> updateModel(StackedModel<IS, IA, O, AM> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return runOnEnsemble(
            ensemble -> {
                int i = 0;
                List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>> res = new ArrayList<>();
                for (Model<IS, ?> submodel : mdl.submodels()) {
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

    @Override public boolean checkState(StackedModel<IS, IA, O, AM> mdl) {
        boolean res = true;
        int i = 0;
        for (Model<IS, ?> submodel : mdl.submodels()) {
            res &= ensemble.get(i).checkState(submodel);
            i++;
        }

        return res && aggregatingTrainer.checkState(mdl.aggregatingModel());
    }

    @Override public StackedDatasetTrainer<IS, IA, O, L, AM> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        super.withEnvironmentBuilder(envBuilder);

        return this;
    }

    protected <K, V> StackedModel<IS, IA, O, AM> runOnEnsemble(
        IgniteFunction<List<TrainerWithConverters<IA, IS, ?, L, AM>>, List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>>> taskSupplier,
        IgniteBiFunction<DatasetTrainer<AM, L>, IgniteBiFunction<K, V, Vector>, AM> aggregatorProcessor,
        IgniteBiFunction<K, V, Vector> featureExtractor) {

        List<IgniteSupplier<ModelWithConverters<IA, IS, ?, AM>>> mdlSuppliers = taskSupplier.apply(ensemble);

        List<ModelWithConverters<IA, IS, ?, AM>> subMdls = environment.parallelismStrategy().submit(mdlSuppliers).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        // Add new columns consisting in submodels output in features.
        IgniteBiFunction<K, V, Vector> augmentedExtractor = featureExtractor.andThen((Vector v) -> {
            Vector[] objects = subMdls.stream().map(sm -> sm.applyToVector(v)).toArray(Vector[]::new);
            return VectorUtils.concat(v, objects);
        });

        AM aggregator = aggregatorProcessor.apply(aggregatingTrainer, augmentedExtractor);

        StackedModel<IS, IA, O, AM> res = new StackedModel<>(
            aggregator,
            aggregatingInputMerger,
            subMdls.get(0).wrapped(),
            submodelInput2AggregatingInputConverter);

        for (ModelWithConverters<IA, IS, ?, AM> subMdl : subMdls)
            subMdl.addToStackedModel(res);

        return res;
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

        protected boolean checkState(Model<I, ?> mdl) {
            return trainer.checkState((Model<I, O>)mdl);
        }

        public <K, V> ModelWithConverters<IA, I, O, AM> fit(DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featuresExtractor,
            IgniteBiFunction<K, V, L> lbExtractor) {
            Model<I, O> mdl = trainer.fit(datasetBuilder, featuresExtractor, lbExtractor);
            return new ModelWithConverters<>(mdl,
                inputConverter,
                outputConverter,
                outputToAggregatingInputConverter);
        }

        protected <K, V> ModelWithConverters<IA, I, O, AM> updateModel(Model<I, ?> mdl,
            DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featureExtractor,
            IgniteBiFunction<K, V, L> lbExtractor) {
            Model<I, O> updatedMdl = trainer.update((Model<I, O>)mdl, datasetBuilder, featureExtractor, lbExtractor);
            return new ModelWithConverters<>(updatedMdl,
                inputConverter,
                outputConverter, outputToAggregatingInputConverter);
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
            stackedMdl.withAddedSubmodel(i -> mdl.andThen(outputToAggregatingInputConverter).apply(i));
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
