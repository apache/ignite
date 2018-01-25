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

package org.apache.ignite.ml.dlc.dataset.part.recoverable;

/**
 * Recoverable part of a DLC dataset partition contains matrix with features in a dense flat column-major format.
 */
public class DLCDatasetPartitionRecoverable implements AutoCloseable {
    /** Matrix with features in a dense flat column-major format. */
    private final double[] features;

    /** Number of rows. */
    private final int rows;

    /** Number of columns. */
    private final int cols;

    /**
     * Constructs a new instance of recoverable data of DLC dataset partition.
     *
     * @param features matrix with features in a dense flat column-major format
     * @param rows number of rows
     * @param cols number of columns
     */
    public DLCDatasetPartitionRecoverable(double[] features, int rows, int cols) {
        this.features = features;
        this.rows = rows;
        this.cols = cols;
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

    /** {@inheritDoc} */
    @Override public void close() {
        // do nothing
    }
}
