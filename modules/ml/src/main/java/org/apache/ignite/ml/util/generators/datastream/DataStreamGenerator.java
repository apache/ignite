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

package org.apache.ignite.ml.util.generators.datastream;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.LabeledVector;

public interface DataStreamGenerator {
    Stream<LabeledVector<Vector, Double>> labeled();

    default Stream<Vector> unlabeled() {
        return labeled().map(DatasetRow::features);
    }

    default Stream<LabeledVector<Vector, Double>> labeled(IgniteFunction<Vector, Double> classifier) {
        return labeled().map(DatasetRow::features).map(v -> new LabeledVector<>(v, classifier.apply(v)));
    }

    default Map<Vector, Double> asMap(int datasetSize) {
        return labeled().limit(datasetSize)
            .collect(Collectors.toMap(DatasetRow::features, LabeledVector::label));
    }

    default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, int partitions) {
        return new DatasetBuilderAdapter(this, datasetSize, partitions);
    }

    default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, IgniteBiPredicate<Vector, Double> filter,
        int partitions, UpstreamTransformerBuilder<Vector, Double> upstreamTransformerBuilder) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions, upstreamTransformerBuilder);
    }

    default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, IgniteBiPredicate<Vector, Double> filter,
        int partitions) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions);
    }

    public static class DatasetBuilderAdapter extends LocalDatasetBuilder<Vector, Double> {
        public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize, int partitions) {
            super(generator.asMap(datasetSize), partitions);
        }

        public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize,
            IgniteBiPredicate<Vector, Double> filter, int partitions,
            UpstreamTransformerBuilder<Vector, Double> upstreamTransformerBuilder) {

            super(generator.asMap(datasetSize), filter, partitions, upstreamTransformerBuilder);
        }

        public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize,
            IgniteBiPredicate<Vector, Double> filter, int partitions) {

            super(generator.asMap(datasetSize), filter, partitions);
        }
    }
}
