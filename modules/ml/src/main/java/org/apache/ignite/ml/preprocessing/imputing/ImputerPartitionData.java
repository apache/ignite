/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    /** Sum of values in partition. */
    private double[] sums;

    /** Count of values in partition. */
    private int[] counts;

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
     * Gets the array of amounts of values in partition for each feature in the dataset.
     *
     * @return The counts.
     */
    public int[] counts() {
        return counts;
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
