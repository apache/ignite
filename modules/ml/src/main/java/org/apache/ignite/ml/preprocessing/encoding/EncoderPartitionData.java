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

package org.apache.ignite.ml.preprocessing.encoding;

import java.util.Map;

/**
 * Partition data used in Encoder preprocessor.
 */
public class EncoderPartitionData implements AutoCloseable {
    /** Frequencies of categories for each categorial feature presented as strings. */
    private Map<String, Integer>[] categoryFrequencies;

    /** Frequencies of categories for label presented as strings. */
    private Map<String, Integer> labelFrequencies;

    /**
     * Constructs a new instance of String Encoder partition data.
     */
    public EncoderPartitionData() {
    }

    /**
     * Gets the array of maps of frequencies by value in partition for each feature in the dataset.
     *
     * @return The frequencies.
     */
    public Map<String, Integer>[] categoryFrequencies() {
        return categoryFrequencies;
    }

    /**
     * Gets the map of frequencies by value in partition for label.
     *
     * @return The frequencies.
     */
    public Map<String, Integer> labelFrequencies() {
        return labelFrequencies;
    }

    /**
     * Sets the array of maps of frequencies by value in partition for each feature in the dataset.
     *
     * @param categoryFrequencies The given value.
     * @return The partition data.
     */
    public EncoderPartitionData withCategoryFrequencies(Map<String, Integer>[] categoryFrequencies) {
        this.categoryFrequencies = categoryFrequencies;
        return this;
    }

    /**
     * Sets the map of frequencies by value in partition for label.
     *
     * @param labelFrequencies The given value.
     * @return The partition data.
     */
    public EncoderPartitionData withLabelFrequencies(Map<String, Integer> labelFrequencies) {
        this.labelFrequencies = labelFrequencies;
        return this;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
