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
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class CompositionUtils {
    public static <I, O, M extends Model<I, O>, L> DatasetTrainer<Model<I, O>, L> unsafeCoerce(
        DatasetTrainer<? extends M, L> trainer) {
        return new DatasetTrainer<Model<I, O>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V> Model<I, O> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return trainer.fit(datasetBuilder, featureExtractor, lbExtractor);
            }

            /** {@inheritDoc} */
            @Override public <K, V> Model<I, O> update(Model<I, O> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                DatasetTrainer<Model<I, O>, L> trainer1 = (DatasetTrainer<Model<I, O>, L>)trainer;
                return trainer1.update(mdl, datasetBuilder, featureExtractor, lbExtractor);
            }

            /** {@inheritDoc} */
            @Override protected boolean checkState(Model<I, O> mdl) {
                return true;
            }

            /** {@inheritDoc} */
            @Override protected <K, V> Model<I, O> updateModel(Model<I, O> mdl, DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return null;
            }
        };
    }
}
