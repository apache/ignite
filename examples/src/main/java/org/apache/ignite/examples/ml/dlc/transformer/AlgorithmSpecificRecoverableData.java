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

package org.apache.ignite.examples.ml.dlc.transformer;

/**
 * Algorithm specific recoverable data consists of feature matrix supplemented by the column containing "1" and labels.
 */
public class AlgorithmSpecificRecoverableData implements AutoCloseable {
    /** Matrix supplemented by the column containing "1". */
    private final double[] a;

    /** Number of rows. */
    private final int rows;

    /** Number of columns. */
    private final int cols;

    /** Labels. */
    private final double[] b;

    /**
     * Constructs a new instance of algorithm specific recoverable data.
     *
     * @param a matrix supplemented by the column containing "1"
     * @param b labels
     */
    public AlgorithmSpecificRecoverableData(double[] a, int rows, int cols, double[] b) {
        this.a = a;
        this.rows = rows;
        this.cols = cols;
        this.b = b;
    }

    /** */
    public double[] getA() {
        return a;
    }

    /** */
    public double[] getB() {
        return b;
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
    @Override public void close() {
        // do nothing
    }
}
