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

package org.apache.ignite.ml.trainers;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.composition.combinators.sequential.TrainersSequentialComposition;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Type used to adapt input and output types of wrapped {@link DatasetTrainer}. Produces model which is composition  of
 * form {@code before `andThen` wMdl `andThen` after} where wMdl is model produced by wrapped trainer.
 *
 * @param <I> Input type of model produced by this trainer.
 * @param <O> Output type of model produced by this trainer.
 * @param <IW> Input type of model produced by wrapped trainer.
 * @param <OW> Output type of model produced by wrapped trainer.
 * @param <M> Type of model produced by wrapped model.
 * @param <L> Type of labels.
 */
public class AdaptableDatasetTrainer<I, O, IW, OW, M extends IgniteModel<IW, OW>, L>
    extends DatasetTrainer<AdaptableDatasetModel<I, O, IW, OW, M>, L> {
    /** Wrapped trainer. */
    private final DatasetTrainer<M, L> wrapped;

    /** Function used to convert input type of wrapped trainer. */
    private final IgniteFunction<I, IW> before;

    /** Function used to convert output type of wrapped trainer. */
    private final IgniteFunction<OW, O> after;

    /** Function which is applied after feature extractor. */
    private final IgniteFunction<LabeledVector<L>, LabeledVector<L>> afterExtractor;

    /** Upstream transformer builder which will be used in dataset builder. */
    private final UpstreamTransformerBuilder upstreamTransformerBuilder;

    /**
     * Construct instance of this class from a given {@link DatasetTrainer}.
     *
     * @param wrapped Wrapped trainer.
     * @param <I> Input type of wrapped trainer.
     * @param <O> Output type of wrapped trainer.
     * @param <M> Type of model produced by wrapped trainer.
     * @param <L> Type of labels.
     * @return Instance of this class.
     */
    public static <I, O, M extends IgniteModel<I, O>, L> AdaptableDatasetTrainer<I, O, I, O, M, L> of(
        DatasetTrainer<M, L> wrapped) {
        return new AdaptableDatasetTrainer<>(IgniteFunction.identity(),
            wrapped,
            IgniteFunction.identity(),
            IgniteFunction.identity(),
            UpstreamTransformerBuilder.identity());
    }

    /**
     * Construct instance of this class with specified wrapped trainer and converter functions.
     *
     * @param before Function used to convert input type of wrapped trainer.
     * @param wrapped Wrapped trainer.
     * @param builder Upstream transformer builder which will be used in dataset builder.
     */
    private AdaptableDatasetTrainer(IgniteFunction<I, IW> before, DatasetTrainer<M, L> wrapped,
        IgniteFunction<OW, O> after,
        IgniteFunction<LabeledVector<L>, LabeledVector<L>> afterExtractor,
        UpstreamTransformerBuilder builder) {
        this.before = before;
        this.wrapped = wrapped;
        this.after = after;
        this.afterExtractor = afterExtractor;
        upstreamTransformerBuilder = builder;
    }

    /** {@inheritDoc} */
    @Override public <K, V> AdaptableDatasetModel<I, O, IW, OW, M> fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
                                                                       Preprocessor<K, V> extractor) {
        M fit = wrapped.
            withEnvironmentBuilder(envBuilder)
            .fit(datasetBuilder.withUpstreamTransformer(upstreamTransformerBuilder),
                extractor.map(afterExtractor));

        return new AdaptableDatasetModel<>(before, fit, after);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(AdaptableDatasetModel<I, O, IW, OW, M> mdl) {
        return wrapped.isUpdateable(mdl.innerModel());
    }

    /** {@inheritDoc} */
    @Override protected <K, V> AdaptableDatasetModel<I, O, IW, OW, M> updateModel(
        AdaptableDatasetModel<I, O, IW, OW, M> mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {
       M updated = wrapped.withEnvironmentBuilder(envBuilder)
            .updateModel(
                mdl.innerModel(),
                datasetBuilder.withUpstreamTransformer(upstreamTransformerBuilder),
                extractor.map(afterExtractor));

        return mdl.withInnerModel(updated);
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = mdl `andThen` after}.
     *
     * @param after Function inserted before produced model.
     * @param <O1> Type of produced model output.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <O1> AdaptableDatasetTrainer<I, O1, IW, OW, M, L> afterTrainedModel(IgniteFunction<O, O1> after) {
        return new AdaptableDatasetTrainer<>(before,
            wrapped,
            i -> after.apply(this.after.apply(i)),
            afterExtractor,
            upstreamTransformerBuilder);
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = f `andThen` mdl}.
     *
     * @param before Function inserted before produced model.
     * @param <I1> Type of produced model input.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <I1> AdaptableDatasetTrainer<I1, O, IW, OW, M, L> beforeTrainedModel(IgniteFunction<I1, I> before) {
        IgniteFunction<I1, IW> function = i -> this.before.apply(before.apply(i));
        return new AdaptableDatasetTrainer<>(function,
            wrapped,
            after,
            afterExtractor,
            upstreamTransformerBuilder);
    }

    /**
     * Specify {@link DatasetMapping} which will be applied to dataset before fitting and updating.
     *
     * @param mapping {@link DatasetMapping} which will be applied to dataset before fitting and updating.
     * @return New trainer of the same type, but with specified mapping applied to dataset before fitting and updating.
     */
    public AdaptableDatasetTrainer<I, O, IW, OW, M, L> withDatasetMapping(DatasetMapping<L, L> mapping) {
        return of(new DatasetTrainer<M, L>() {
            /** {@inheritDoc} */
            @Override public <K, V> M fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> extractor) {
                return wrapped.fit(datasetBuilder, extractor.map(lv -> new LabeledVector<>(
                    mapping.mapFeatures(lv.features()),
                    mapping.mapLabels((L)lv.label())
                )));
            }

            /** {@inheritDoc} */
            @Override public <K, V> M update(M mdl, DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> vectorizer) {
                return wrapped.update(mdl, datasetBuilder, vectorizer.map(lv -> new LabeledVector<>(
                    mapping.mapFeatures(lv.features()),
                    mapping.mapLabels((L)lv.label())
                )));
            }

            /** {@inheritDoc} */
            @Override public boolean isUpdateable(M mdl) {
                return false;
            }

            @Override protected <K, V> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {
                return null;
            }

        }).beforeTrainedModel(before).afterTrainedModel(after);
    }

    /**
     * Create a {@link TrainersSequentialComposition} of this trainer and specified trainer.
     *
     * @param tr Trainer to compose with.
     * @param datasetMappingProducer {@link DatasetMapping} producer specifying dependency between this trainer and
     * trainer to compose with.
     * @param <O1> Type of output of trainer to compose with.
     * @param <M1> Type of model produced by the trainer to compose with.
     * @return A {@link TrainersSequentialComposition} of this trainer and specified trainer.
     */
    public <O1, M1 extends IgniteModel<O, O1>> TrainersSequentialComposition<I, O, O1, L> andThen(
        DatasetTrainer<M1, L> tr,
        IgniteFunction<AdaptableDatasetModel<I, O, IW, OW, M>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> datasetMappingProducer) {
        IgniteFunction<IgniteModel<I, O>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> coercedMapping = mdl ->
            datasetMappingProducer.apply((AdaptableDatasetModel<I, O, IW, OW, M>)mdl);
        return new TrainersSequentialComposition<>(this,
            tr,
            coercedMapping);
    }

    /**
     * Specify function which will be applied after feature extractor.
     *
     * @param after Function which will be applied after feature extractor.
     * @return New trainer with same parameters as this trainer except that specified function will be applied after
     * feature extractor.
     */
    public AdaptableDatasetTrainer<I, O, IW, OW, M, L> afterFeatureExtractor(IgniteFunction<Vector, Vector> after) {
        IgniteFunction<LabeledVector<L>, LabeledVector<L>> newExtractor = afterExtractor.andThen((IgniteFunction<LabeledVector<L>, LabeledVector<L>>)
            slv -> new LabeledVector<>(after.apply(slv.features()), slv.label()));
        return new AdaptableDatasetTrainer<>(before,
            wrapped,
            this.after,
            newExtractor,
            upstreamTransformerBuilder);
    }

    /**
     * Specify function which will be applied after label extractor.
     *
     * @param after Function which will be applied after label extractor.
     * @return New trainer with same parameters as this trainer has except that specified function will be applied after
     * label extractor.
     */
    public AdaptableDatasetTrainer<I, O, IW, OW, M, L> afterLabelExtractor(IgniteFunction<L, L> after) {
        IgniteFunction<LabeledVector<L>, LabeledVector<L>> newExtractor = afterExtractor.andThen((IgniteFunction<LabeledVector<L>, LabeledVector<L>>)
            slv -> new LabeledVector<>(slv.features(), after.apply(slv.label())));
        return new AdaptableDatasetTrainer<>(before,
            wrapped,
            this.after,
            newExtractor,
            upstreamTransformerBuilder);
    }

    /**
     * Specify which {@link UpstreamTransformerBuilder} will be used.
     *
     * @param upstreamTransformerBuilder {@link UpstreamTransformerBuilder} to use.
     * @return New trainer with same parameters as this trainer has except that specified {@link
     * UpstreamTransformerBuilder} will be used.
     */
    public AdaptableDatasetTrainer<I, O, IW, OW, M, L> withUpstreamTransformerBuilder(
        UpstreamTransformerBuilder upstreamTransformerBuilder) {
        return new AdaptableDatasetTrainer<>(before,
            wrapped,
            after,
            afterExtractor,
            upstreamTransformerBuilder);
    }
}
