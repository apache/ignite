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

package org.apache.ignite.ml.math.isolve;

/**
 * On Heap partition data that keeps part of a linear system.
 */
public class LinSysPartitionDataOnHeap implements AutoCloseable {
    /** Part of X matrix. */
    private final double[] x;

    /** Part of Y vector. */
    private final double[] y;

    /** Number of rows. */
    private final int rows;

    /**
     * Constructs a new instance of linear system partition data.
     *
     * @param x Part of X matrix.
     * @param y Part of Y vector.
     * @param rows Number of rows.
     */
    public LinSysPartitionDataOnHeap(double[] x, double[] y, int rows) {
        this.x = x;
        this.rows = rows;
        this.y = y;
    }

    /** */
    public double[] getX() {
        return x;
    }

    /** */
    public int getRows() {
        return rows;
    }

    /** */
    public double[] getY() {
        return y;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
