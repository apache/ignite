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

package org.apache.ignite.ml.util.generators;

import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * DataStreamGenerator to DatasetBuilder adapter.
 */
class DatasetBuilderAdapter extends LocalDatasetBuilder<Vector, Double> {
    /**
     * Constructs an instance of DatasetBuilderAdapter.
     *
     * @param generator Generator.
     * @param datasetSize Dataset size.
     * @param partitions Partitions.
     */
    public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize, int partitions) {
        super(generator.asMap(datasetSize), partitions);
    }

    /**
     * Constructs an instance of DatasetBuilderAdapter.
     *
     * @param generator Generator.
     * @param datasetSize Dataset size.
     * @param filter Filter.
     * @param partitions Partitions.
     * @param upstreamTransformerBuilder Upstream transformer builder.
     */
    public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize,
        IgniteBiPredicate<Vector, Double> filter, int partitions,
        UpstreamTransformerBuilder upstreamTransformerBuilder) {

        super(generator.asMap(datasetSize), filter, partitions, upstreamTransformerBuilder);
    }

    /**
     * Constructs an instance of DatasetBuilderAdapter.
     *
     * @param generator Generator.
     * @param datasetSize Dataset size.
     * @param filter Filter.
     * @param partitions Partitions.
     */
    public DatasetBuilderAdapter(DataStreamGenerator generator, int datasetSize,
        IgniteBiPredicate<Vector, Double> filter, int partitions) {

        super(generator.asMap(datasetSize), filter, partitions);
    }
}
