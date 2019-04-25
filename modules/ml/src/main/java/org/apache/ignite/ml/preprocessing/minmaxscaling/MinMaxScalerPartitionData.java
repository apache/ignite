/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.preprocessing.minmaxscaling;

/**
 * Partition data used in minmaxscaling preprocessor.
 *
 * @see MinMaxScalerTrainer
 * @see MinMaxScalerPreprocessor
 */
public class MinMaxScalerPartitionData implements AutoCloseable {
    /** Minimal values. */
    private final double[] min;

    /** Maximum values. */
    private final double[] max;

    /**
     * Constructs a new instance of minmaxscaling partition data.
     *
     * @param min Minimal values.
     * @param max Maximum values.
     */
    public MinMaxScalerPartitionData(double[] min, double[] max) {
        this.min = min;
        this.max = max;
    }

    /** */
    public double[] getMin() {
        return min;
    }

    /** */
    public double[] getMax() {
        return max;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
