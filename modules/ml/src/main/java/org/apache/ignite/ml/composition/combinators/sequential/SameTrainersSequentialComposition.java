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
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class SameTrainersSequentialComposition<I, O, M extends Model<I, O>, L, T extends DatasetTrainer<M, L>>
    extends DatasetTrainer<SameModelsSequentialComposition<I, O, M>, L> {
    private final T initialTrainer;
    private final IgniteBiFunction<Integer, M, T> trainers;
    private IgnitePredicate<SameModelsSequentialComposition<I, O, M>> isConverged;
    private final IgniteFunction<M, DatasetMapping<L, L>> mapping;
    private final IgniteFunction<O, I> f;

    public SameTrainersSequentialComposition(
        T initialTrainer,
        IgniteBiFunction<Integer, M, T> trainers,
        IgniteFunction<M, DatasetMapping<L, L>> mapping,
        IgniteFunction<O, I> f) {
        this.initialTrainer = initialTrainer;
        this.trainers = trainers;
        this.mapping = mapping;
        this.f = f;
    }

    /** {@inheritDoc} */
    @Override public <K, V> SameModelsSequentialComposition<I, O, M> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        T curTrainer = initialTrainer;
        M curMdl;
        int i = 0;
        curMdl = curTrainer.fit(datasetBuilder, featureExtractor, lbExtractor);
        SameModelsSequentialComposition<I, O, M> curComposition = new SameModelsSequentialComposition<>(
            f,
            Collections.singletonList(curMdl)
        );
        i++;

        while (!isConverged.apply(curComposition)) {
            curTrainer = trainers.apply(i, curMdl);
            DatasetMapping<L, L> dsMapping = mapping.apply(curMdl);
            curMdl = curTrainer.fit(datasetBuilder,
                featureExtractor.andThen((IgniteFunction<? super Vector, ? extends Vector>)dsMapping::mapFeatures),
                lbExtractor.andThen((IgniteFunction<? super L, ? extends L>)dsMapping::mapLabels));
            curComposition.addModel(curMdl);
            i++;
        }

        return curComposition;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(SameModelsSequentialComposition<I, O, M> mdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> SameModelsSequentialComposition<I, O, M> updateModel(
        SameModelsSequentialComposition<I, O, M> mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }
}
