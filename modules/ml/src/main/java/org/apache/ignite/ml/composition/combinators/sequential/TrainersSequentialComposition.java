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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.DatasetMapping;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TrainersSequentialComposition<I, O1, M1 extends Model<I, O1>, L, T1 extends DatasetTrainer<M1, L>,
    O2, M2 extends Model<O1, O2>, T2 extends DatasetTrainer<M2, L>>
    extends DatasetTrainer<ModelsSequentialComposition<I, O1, M1, O2, M2>, L> {
    private T1 tr1;
    private T2 tr2;
    private IgniteFunction<M1, DatasetMapping<L, L>> datasetMapping;

    public TrainersSequentialComposition(T1 tr1, T2 tr2,
        IgniteFunction<M1, DatasetMapping<L, L>> datasetMapping) {
        this.tr1 = tr1;
        this.tr2 = tr2;
        this.datasetMapping = datasetMapping;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, M1, O2, M2> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        M1 mdl1 = tr1.fit(datasetBuilder, featureExtractor, lbExtractor);
        DatasetMapping<L, L> mapping = datasetMapping.apply(mdl1);

        M2 mdl2 = tr2.fit(datasetBuilder,
            featureExtractor.andThen(mapping::mapFeatures),
            lbExtractor.andThen(mapping::mapLabels));

        return new ModelsSequentialComposition<>(mdl1, mdl2);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsSequentialComposition<I, O1, M1, O2, M2> update(
        ModelsSequentialComposition<I, O1, M1, O2, M2> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return super.update(mdl, datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(ModelsSequentialComposition<I, O1, M1, O2, M2> mdl) {
        // Never called.
        throw new NotImplementedException();
    }

    /** {@inheritDoc} */
    @Override protected <K, V> ModelsSequentialComposition<I, O1, M1, O2, M2> updateModel(
        ModelsSequentialComposition<I, O1, M1, O2, M2> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Never called.
        throw new NotImplementedException();
    }

    public static <L, I, O> DatasetMapping<L, L> augmentFeatures(Model<I, O> mdl,
        IgniteFunction<Vector, I> vector2FirstInputConvertor,
        IgniteFunction<O, Vector> firstOutput2VectorConvertor) {
        return new DatasetMapping<L, L>() {
            @Override public Vector mapFeatures(Vector v) {
                return VectorUtils.concat(v,
                    vector2FirstInputConvertor.andThen(mdl).andThen(firstOutput2VectorConvertor).apply(v));
            }

            @Override public L mapLabels(L lbls) {
                return lbls;
            }
        };
    }

    public DatasetTrainer<Model<I, O2>, L> unsafeSimplyTyped() {
        TrainersSequentialComposition<I, O1, M1, L, T1, O2, M2, T2> self = this;
        return new DatasetTrainer<Model<I, O2>, L>() {
            @Override
            public <K, V> Model<I, O2> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return self.fit(datasetBuilder, featureExtractor, lbExtractor);
            }

            @Override public <K, V> Model<I, O2> update(Model<I, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return self.update((ModelsSequentialComposition<I, O1, M1, O2, M2>)mdl, datasetBuilder, featureExtractor, lbExtractor);
            }

            @Override protected boolean checkState(Model<I, O2> mdl) {
                return false;
            }

            @Override protected <K, V> Model<I, O2> updateModel(Model<I, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return null;
            }
        }
    }

    private static <K, V, I, O> IgniteBiFunction<K, V, Vector> getNewExtractor(IgniteBiFunction<K, V, Vector> oldExtractor,
        Model<I, O> mdl,
        IgniteFunction<Vector, I> vector2FirstInputConvertor,
        IgniteFunction<O, Vector> firstOutput2VectorConvertor) {
        return oldExtractor.andThen(vector2FirstInputConvertor).andThen(mdl).andThen(firstOutput2VectorConvertor);
    }
}
