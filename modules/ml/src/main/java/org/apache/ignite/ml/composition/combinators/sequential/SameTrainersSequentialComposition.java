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

import java.util.Collections;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.AdaptableDatasetTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * This class represents sequential composition of trainers which is itself is a trainer.
 * This trainer acts in the following way:
 * // TODO:
 *
 * @param <I>
 * @param <O>
 * @param <L>
 */
public class SameTrainersSequentialComposition<I, O, L>
    extends DatasetTrainer<IgniteModel<I, O>, L> {
    private final DatasetTrainer<IgniteModel<I, O>, L> initialTrainer;
    private final IgniteBiFunction<Integer, IgniteModel<I, O>, DatasetTrainer<IgniteModel<I, O>, L>> trainerProducer;
    private IgnitePredicate<IgniteModel<I, O>> isConverged;
    private final IgniteFunction<IgniteModel<I, O>, DatasetMapping<L, L>> mappingProducer;
    private final IgniteFunction<O, I> f;

    public SameTrainersSequentialComposition(
        IgniteBiFunction<Integer, IgniteModel<I, O>, DatasetTrainer<IgniteModel<I, O>, L>> trainerProducer,
        IgniteFunction<IgniteModel<I, O>, DatasetMapping<L, L>> mappingProducer,
        IgniteFunction<O, I> f) {
        initialTrainer = CompositionUtils.unsafeCoerce(trainerProducer.apply(0, null));
        this.trainerProducer = trainerProducer;
        this.mappingProducer = mappingProducer;
        this.f = f;
    }

    public static <I, O, L> SameTrainersSequentialComposition<I, O, L> of(DatasetTrainer<? extends IgniteModel<I, O>, L> tr1,
        DatasetTrainer<? extends IgniteModel<I, O>, L> tr2,
        IgniteFunction<IgniteModel<I, O>, DatasetMapping<L, L>> mappingProducer,
        IgniteFunction<O, I> f) {
        return new SameTrainersSequentialComposition<>(
            (integer, model) ->
                CompositionUtils.unsafeCoerce(integer == 0 ? tr1 : AdaptableDatasetTrainer.of(tr2).withDatasetMapping(mappingProducer.apply(model))),
            mappingProducer,
            f);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteModel<I, O> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        DatasetTrainer<IgniteModel<I, O>, L> curTrainer = initialTrainer;
        int i = 0;
        IgniteModel<I, O> curMdl = curTrainer.fit(datasetBuilder, featureExtractor, lbExtractor);
        SameModelsSequentialComposition<I, O> curComposition = new SameModelsSequentialComposition<>(
            f,
            Collections.singletonList(curMdl)
        );
        i++;

        while (!isConverged.apply(curComposition)) {
            curTrainer = trainerProducer.apply(i, curMdl);
            DatasetMapping<L, L> dsMapping = mappingProducer.apply(curMdl);
            curMdl = curTrainer.fit(datasetBuilder,
                featureExtractor.andThen((IgniteFunction<? super Vector, ? extends Vector>)dsMapping::mapFeatures),
                lbExtractor.andThen((IgniteFunction<? super L, ? extends L>)dsMapping::mapLabels));

            curComposition.addModel(curMdl);
            i++;
        }

        return curComposition;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(IgniteModel<I, O> mdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> IgniteModel<I, O> updateModel(
        IgniteModel<I, O> mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }
}
