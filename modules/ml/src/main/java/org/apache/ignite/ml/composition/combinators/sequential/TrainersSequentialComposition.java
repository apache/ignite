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

package org.apache.ignite.ml.composition.combinators.sequential;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * Sequential composition of trainers.
 * Sequential composition of trainers is itself trainer which produces {@link ModelsSequentialComposition}.
 * Training is done in following fashion:
 * <pre>
 *     1. First trainer is trained and `mdl1` is produced.
 *     2. From `mdl1` {@link DatasetMapping} is constructed. This mapping `dsM` encapsulates dependency between first
 *     training result and second trainer.
 *     3. Second trainer is trained using dataset acquired from application `dsM` to original dataset; `mdl2` is produced.
 *     4. `mdl1` and `mdl2` are composed into {@link ModelsSequentialComposition}.
 * </pre>
 *
 * @param <I> Type of input of model produced by first trainer.
 * @param <O1> Type of output of model produced by first trainer.
 * @param <O2> Type of output of model produced by second trainer.
 * @param <L> Type of labels.
 */
public class TrainersSequentialComposition<I, O1, O2, L> extends DatasetTrainer<ModelsSequentialComposition<I, O1, O2>, L> {
    /** First trainer. */
    private DatasetTrainer<IgniteModel<I, O1>, L> tr1;

    /** Second trainer. */
    private DatasetTrainer<IgniteModel<O1, O2>, L> tr2;

    /** Dataset mapping. */
    protected IgniteBiFunction<Integer, ? super IgniteModel<I, O1>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>>
        datasetMapping;

    /**
     * Construct sequential composition of same trainers.
     *
     * @param tr Trainer used for sequential composition.
     * @param datasetMapping Dataset mapping.
     * @param shouldStop Predicate depending on index and model produced by the last trainer
     * indicating if composition process should stop
     * @param out2In Function for conversion of output of model into input of next model.
     * @param <I> Type of input of model produced by trainer.
     * @param <O> Type of output of model produced by trainer.
     * @param <L> Type of labels for trainer.
     * @return Sequential composition of same trainers.
     */
    public static <I, O, L> TrainersSequentialComposition<I, O, O, L> ofSame(
        DatasetTrainer<? extends IgniteModel<I, O>, L> tr,
        IgniteBiFunction<Integer, ? super IgniteModel<I, O>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> datasetMapping,
        IgniteBiPredicate<Integer, IgniteModel<I, O>> shouldStop,
        IgniteFunction<O, I> out2In) {
        return new SameTrainersSequentialComposition<>(CompositionUtils.unsafeCoerce(tr),
            datasetMapping,
            shouldStop,
            out2In);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, O2> fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {

        IgniteModel<I, O1> mdl1 = tr1.fit(datasetBuilder, preprocessor);
        IgniteFunction<LabeledVector<L>, LabeledVector<L>> mapping = datasetMapping.apply(0, mdl1);

        IgniteModel<O1, O2> mdl2 = tr2.fit(datasetBuilder, preprocessor.map(mapping));

        return new ModelsSequentialComposition<>(mdl1, mdl2);
    }

    /**
     * Construct sequential composition of given two trainers.
     *
     * @param tr1 First trainer.
     * @param tr2 Second trainer.
     * @param datasetMapping Dataset mapping.
     */
    public TrainersSequentialComposition(DatasetTrainer<? extends IgniteModel<I, O1>, L> tr1,
        DatasetTrainer<? extends IgniteModel<O1, O2>, L> tr2,
        IgniteFunction<? super IgniteModel<I, O1>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> datasetMapping) {
        this.tr1 = CompositionUtils.unsafeCoerce(tr1);
        this.tr2 = CompositionUtils.unsafeCoerce(tr2);
        this.datasetMapping = (i, mdl) -> datasetMapping.apply(mdl);
    }

    /**
     * Create sequential composition of two trainers.
     * @param tr1 First trainer.
     * @param tr2 Second trainer.
     * @param datasetMapping Dataset mapping containing dependence between first and second trainer.
     */
    public TrainersSequentialComposition(DatasetTrainer<? extends IgniteModel<I, O1>, L> tr1,
        DatasetTrainer<? extends IgniteModel<O1, O2>, L> tr2,
        IgniteBiFunction<Integer, ? super IgniteModel<I, O1>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> datasetMapping) {
        this.tr1 = CompositionUtils.unsafeCoerce(tr1);
        this.tr2 = CompositionUtils.unsafeCoerce(tr2);
        this.datasetMapping = datasetMapping;
    }

    /**
     * This method is never called, instead of constructing logic of update from
     * {@link DatasetTrainer#isUpdateable(IgniteModel)} and
     * {@link DatasetTrainer#updateModel(IgniteModel, DatasetBuilder, Preprocessor)}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return Updated model.
     */
    @Override protected <K, V> ModelsSequentialComposition<I, O1, O2> updateModel(
        ModelsSequentialComposition<I, O1, O2> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        // Never called.
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, O2> update(
        ModelsSequentialComposition<I, O1, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {

        IgniteModel<I, O1> firstUpdated = tr1.update(mdl.firstModel(), datasetBuilder, preprocessor);
        IgniteFunction<LabeledVector<L>, LabeledVector<L>> mapping = datasetMapping.apply(0, firstUpdated);

        IgniteModel<O1, O2> secondUpdated = tr2.update(mdl.secondModel(), datasetBuilder, preprocessor.map(mapping)
        );

        return new ModelsSequentialComposition<>(firstUpdated, secondUpdated);
    }

    /**
     * This method is never called, instead of constructing logic of update from
     * {@link DatasetTrainer#isUpdateable} and
     * {@link DatasetTrainer#updateModel}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return True if current critical for training parameters correspond to parameters from last training.
     */
    @Override public boolean isUpdateable(ModelsSequentialComposition<I, O1, O2> mdl) {
        // Never called.
        throw new IllegalStateException();
    }

    /**
     * Sequential composition of same trainers.
     *
     * @param <I> Type of input of model produced by trainers.
     * @param <O> Type of output of model produced by trainers.
     * @param <L> Type of labels.
     */
    private static class SameTrainersSequentialComposition<I, O, L> extends TrainersSequentialComposition<I, O, O, L> {
        /** Trainer to sequentially compose. */
        private final DatasetTrainer<IgniteModel<I, O>, L> tr;

        /**
         * Predicate depending on index and model produced by the last trainer indicating if composition process should
         * stop
         */
        private final IgniteBiPredicate<Integer, IgniteModel<I, O>> shouldStop;

        /** Function for conversion of output of model into input of next model. */
        private final IgniteFunction<O, I> out2Input;

        /**
         * Create instance of this class.
         *
         * @param tr Trainer to sequentially compose.
         * @param datasetMapping Dataaset mapping.
         * @param shouldStop Predicate depending on index and model produced by the last trainer
         * indicating if composition process should stop.
         * @param out2Input Function for conversion of output of model into input of next model.
         */
        public SameTrainersSequentialComposition(
            DatasetTrainer<IgniteModel<I, O>, L> tr,
            IgniteBiFunction<Integer, ? super IgniteModel<I, O>, IgniteFunction<LabeledVector<L>, LabeledVector<L>>> datasetMapping,
            IgniteBiPredicate<Integer, IgniteModel<I, O>> shouldStop,
            IgniteFunction<O, I> out2Input) {
            super(null, null, datasetMapping);
            this.tr = tr;
            this.shouldStop = (iteration, model) -> iteration != 0 && shouldStop.apply(iteration, model);
            this.out2Input = out2Input;
        }

        /** {@inheritDoc} */
        @Override public <K, V> ModelsSequentialComposition<I, O, O> fit(DatasetBuilder<K, V> datasetBuilder,
            Preprocessor<K, V> preprocessor) {

            int i = 0;
            IgniteModel<I, O> currMdl = null;
            IgniteFunction<LabeledVector<L>, LabeledVector<L>> mapping =
                IgniteFunction.identity();
            List<IgniteModel<I, O>> mdls = new ArrayList<>();

            while (!shouldStop.apply(i, currMdl)) {
                currMdl = tr.fit(datasetBuilder, preprocessor.map(mapping)
                );
                mdls.add(currMdl);
                if (shouldStop.apply(i, currMdl))
                    break;

                mapping = datasetMapping.apply(i, currMdl);

                i++;
            }

            return ModelsSequentialComposition.ofSame(mdls, out2Input);
        }
    }

    /**
     * Performs coersion of this trainer to {@code DatasetTrainer<IgniteModel<I, O2>, L>}.
     *
     * @return Trainer coerced to {@code DatasetTrainer<IgniteModel<I, O>, L>}.
     */
    public DatasetTrainer<IgniteModel<I, O2>, L> unsafeSimplyTyped() {
        return CompositionUtils.unsafeCoerce(this);
    }
}
