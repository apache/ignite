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

package org.apache.ignite.ml.preprocessing.imputing;

import java.util.Map;

/**
 * Partition data used in imputing preprocessor.
 *
 * @see ImputerTrainer
 * @see ImputerPreprocessor
 */
public class ImputerPartitionData implements AutoCloseable {
    /** Sum of values for each feature in partition. */
    private double[] sums;

    /** Count of values for each feature in partition. */
    private int[] counts;

    /** Max value for each feature in partition. */
    private double[] maxs;

    /** Min of values for each feature in partition. */
    private double[] mins;

    /** Most frequent values. */
    private Map<Double, Integer>[] valuesByFreq;

    /**
     * Constructs a new instance of imputing partition data.
     *
     */
    public ImputerPartitionData() {
    }

    /**
     * Gets the array of sums of values in partition for each feature in the dataset.
     *
     * @return The sums.
     */
    public double[] sums() {
        return sums;
    }

    /**
     * Sets the array of sums of values in partition for each feature in the dataset.
     *
     * @param sums The given value.
     *
     * @return The partition data.
     */
    public ImputerPartitionData withSums(double[] sums) {
        this.sums = sums;
        return this;
    }

    /**
     * Sets the array of amounts of values in partition for each feature in the dataset.
     *
     * @param counts The given value.
     *
     * @return The partition data.
     */
    public ImputerPartitionData withCounts(int[] counts) {
        this.counts = counts;
        return this;
    }

    /**
     * Sets the array of maximum of values in partition for each feature in the dataset.
     *
     * @param maxs The given value.
     *
     * @return The partition data.
     */
    public ImputerPartitionData withMaxs(double[] maxs) {
        this.maxs = maxs;
        return this;
    }

    /**
     * Sets the array of minimum of values in partition for each feature in the dataset.
     *
     * @param mins The given value.
     *
     * @return The partition data.
     */
    public ImputerPartitionData withMins(double[] mins) {
        this.mins = mins;
        return this;
    }

    /**
     * Gets the array of amounts of values in partition for each feature in the dataset.
     *
     * @return The counts.
     */
    public int[] counts() {
        return counts;
    }

    /**
     * Gets the array of maximum of values in partition for each feature in the dataset.
     *
     * @return The counts.
     */
    public double[] maxs() {
        return maxs;
    }

    /**
     * Gets the array of minimum of values in partition for each feature in the dataset.
     *
     * @return The counts.
     */
    public double[] mins() {
        return mins;
    }

    /**
     * Gets the array of maps of frequencies by value in partition for each feature in the dataset.
     *
     * @return The frequencies.
     */
    public Map<Double, Integer>[] valuesByFrequency() {
        return valuesByFreq;
    }

    /**
     * Sets the array of maps of frequencies by value in partition for each feature in the dataset.
     *
     * @param valuesByFreq The given value.
     * @return The partition data.
     */
    public ImputerPartitionData withValuesByFrequency(Map<Double, Integer>[] valuesByFreq) {
        this.valuesByFreq = valuesByFreq;
        return this;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
