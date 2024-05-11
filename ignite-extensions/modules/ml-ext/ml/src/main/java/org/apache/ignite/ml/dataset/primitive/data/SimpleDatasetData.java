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

package org.apache.ignite.ml.dataset.primitive.data;

import org.apache.ignite.ml.dataset.primitive.SimpleDataset;

/**
 * A partition {@code data} of the {@link SimpleDataset} containing matrix of features in flat column-major format
 * stored in heap.
 */
public class SimpleDatasetData implements AutoCloseable {
    /** Matrix of features in a dense flat column-major format. */
    private final double[] features;

    /** Number of rows. */
    private final int rows;

    /**
     * Constructs a new instance of partition {@code data} of the {@link SimpleDataset} containing matrix of features in
     * flat column-major format stored in heap.
     *
     * @param features Matrix of features in a dense flat column-major format.
     * @param rows Number of rows.
     */
    public SimpleDatasetData(double[] features, int rows) {
        this.features = features;
        this.rows = rows;
    }

    /** */
    public double[] getFeatures() {
        return features;
    }

    /** */
    public int getRows() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
