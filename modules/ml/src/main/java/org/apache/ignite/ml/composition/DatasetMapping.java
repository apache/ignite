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

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public interface DatasetMapping<L1, L2> {
    public default <K, V> DatasetBuilder<K, V> mapBuilder(DatasetBuilder<K, V> builder) {
        return builder;
    }

    public default Vector mapFeatures(Vector v) {
        return v;
    }

    public L2 mapLabels(L1 lbls);

    public static <L> DatasetMapping<L, L> mappingFeatures(IgniteFunction<Vector, Vector> mapper) {
        return new DatasetMapping<L, L>() {
            @Override public <K, V> DatasetBuilder<K, V> mapBuilder(DatasetBuilder<K, V> builder) {
                return builder;
            }

            @Override public Vector mapFeatures(Vector v) {
                return mapper.apply(v);
            }

            @Override public L mapLabels(L lbls) {
                return lbls;
            }
        };
    }

    public static <K, V, L> DatasetMapping<L, L> mappingBuilder(IgniteFunction<DatasetBuilder<K, V>, DatasetBuilder<K, V>> mapper) {
        return new DatasetMapping<L, L>() {
            @Override public DatasetBuilder<K, V> mapBuilder(DatasetBuilder<K, V> builder) {
                return mapper.apply(builder);
            }

            @Override public Vector mapFeatures(Vector v) {
                return v;
            }

            @Override public L mapLabels(L lbls) {
                return lbls;
            }
        };
    }
}
