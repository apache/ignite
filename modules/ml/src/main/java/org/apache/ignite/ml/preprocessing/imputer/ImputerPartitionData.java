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

package org.apache.ignite.ml.preprocessing.imputer;

import java.util.Map;

/**
 * Partition data used in imputing preprocessor.
 *
 * @see ImputerTrainer
 * @see ImputerPreprocessor
 */
public class ImputerPartitionData implements AutoCloseable {
    /** Sum of values in partition. */
    private double[] sums;

    /** Count of values in partition. */
    private int[] counts;

    /** Most frequent values. */
    private Map<Double, Integer>[] valuesByFrequency;

    /**
     * Constructs a new instance of imputing partition data.
     *
     */
    public ImputerPartitionData() {
    }

    public double[] sums() {
        return sums;
    }

    public ImputerPartitionData withSums(double[] sums) {
        this.sums = sums;
        return this;
    }

    public ImputerPartitionData withCounts(int[] counts) {
        this.counts = counts;
        return this;
    }

    public int[] counts() {
        return counts;
    }

    public Map<Double, Integer>[] valuesByFrequency() {
        return valuesByFrequency;
    }

    public ImputerPartitionData withValuesByFrequency(Map<Double, Integer>[] valuesByFrequency) {
        this.valuesByFrequency = valuesByFrequency;
        return this;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
