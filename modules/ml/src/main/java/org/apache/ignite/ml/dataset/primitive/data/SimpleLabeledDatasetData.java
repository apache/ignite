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

import org.apache.ignite.ml.dataset.primitive.SimpleLabeledDataset;

/**
 * A partition {@code data} of the {@link SimpleLabeledDataset} containing matrix of features in flat column-major
 * format stored in heap and vector of labels stored in heap as well.
 */
public class SimpleLabeledDatasetData implements AutoCloseable {
    /** Matrix with features in a dense flat column-major format. */
    private final double[] features;

    /** Number of rows. */
    private final int rows;

    /** Number of columns. */
    private final int cols;

    /** Vector with labels. */
    private final double[] labels;

    /**
     * Constructs a new instance of partition {@code data} of the {@link SimpleLabeledDataset} containing matrix of
     * features in flat column-major format stored in heap and vector of labels stored in heap as well.
     *
     * @param features Matrix with features in a dense flat column-major format.
     * @param rows Number of rows.
     * @param cols Number of columns.
     * @param labels Vector with labels.
     */
    public SimpleLabeledDatasetData(double[] features, int rows, int cols, double[] labels) {
        this.features = features;
        this.rows = rows;
        this.cols = cols;
        this.labels = labels;
    }

    /** */
    public double[] getFeatures() {
        return features;
    }

    /** */
    public int getRows() {
        return rows;
    }

    /** */
    public int getCols() {
        return cols;
    }

    /** */
    public double[] getLabels() {
        return labels;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
