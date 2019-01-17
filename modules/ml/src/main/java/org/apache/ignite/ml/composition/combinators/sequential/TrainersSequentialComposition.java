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

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * Sequential composition of trainers.
 * Sequential composition of trainers is itself trainer which produces {@link ModelsSequentialComposition}.
 * Training is done in following fashion:
 * <pre>
 *     1. First trainer is trained and `mdl1` is produced.
 *     2. From `mdl1` {@link DatasetMapping} is constructed. This mapping `dsM` encapsulates dependency between first
 *     training result and second trainer.
 *     3. Second trainer is trained using dataset aquired from application `dsM` to original dataset; `mdl2` is produced.
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
    private IgniteFunction<? super IgniteModel<I, O1>, DatasetMapping<L, L>> datasetMapping;

    /**
     * Construct sequential composition of given two trainers.
     *
     * @param tr1 First trainer.
     * @param tr2 Second trainer.
     * @param datasetMapping Dataset mapping.
     */
    public TrainersSequentialComposition(DatasetTrainer<? extends IgniteModel<I, O1>, L> tr1,
        DatasetTrainer<? extends IgniteModel<O1, O2>, L> tr2,
        IgniteFunction<? super IgniteModel<I, O1>, DatasetMapping<L, L>> datasetMapping) {
        this.tr1 = CompositionUtils.unsafeCoerce(tr1);
        this.tr2 = CompositionUtils.unsafeCoerce(tr2);
        this.datasetMapping = datasetMapping;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, O2> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        IgniteModel<I, O1> mdl1 = tr1.fit(datasetBuilder, featureExtractor, lbExtractor);
        DatasetMapping<L, L> mapping = datasetMapping.apply(mdl1);

        IgniteModel<O1, O2> mdl2 = tr2.fit(datasetBuilder,
            featureExtractor.andThen(mapping::mapFeatures),
            lbExtractor.andThen(mapping::mapLabels));

        return new ModelsSequentialComposition<>(mdl1, mdl2);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, O2> update(
        ModelsSequentialComposition<I, O1, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        IgniteModel<I, O1> firstUpdated = tr1.update(mdl.firstModel(), datasetBuilder, featureExtractor, lbExtractor);
        DatasetMapping<L, L> mapping = datasetMapping.apply(firstUpdated);

        IgniteModel<O1, O2> secondUpdated = tr2.update(mdl.secondModel(),
            datasetBuilder,
            featureExtractor.andThen(mapping::mapFeatures),
            lbExtractor.andThen(mapping::mapLabels));

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
     * This method is never called, instead of constructing logic of update from
     * {@link DatasetTrainer#isUpdateable(IgniteModel)} and
     * {@link DatasetTrainer#updateModel(IgniteModel, DatasetBuilder, IgniteBiFunction, IgniteBiFunction)}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return Updated model.
     */
    @Override protected <K, V> ModelsSequentialComposition<I, O1, O2> updateModel(
        ModelsSequentialComposition<I, O1, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Never called.
        throw new IllegalStateException();
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
