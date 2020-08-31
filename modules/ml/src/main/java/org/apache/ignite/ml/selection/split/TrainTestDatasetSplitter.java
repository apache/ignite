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

package org.apache.ignite.ml.selection.split;

import java.io.Serializable;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.selection.split.mapper.UniformMapper;

/**
 * Dataset splitter that splits dataset into train and test subsets.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class TrainTestDatasetSplitter<K, V> implements Serializable {
    /** */
    private static final long serialVersionUID = 3148338796945474491L;

    /** Mapper used to map a key-value pair to a point on the segment (0, 1). */
    private final UniformMapper<K, V> mapper;

    /**
     * Constructs a new instance of train test dataset splitter.
     */
    public TrainTestDatasetSplitter() {
        this(new SHA256UniformMapper<>());
    }

    /**
     * Constructs a new instance of train test dataset splitter.
     *
     * @param mapper Mapper used to map a key-value pair to a point on the segment (0, 1).
     */
    public TrainTestDatasetSplitter(UniformMapper<K, V> mapper) {
        this.mapper = mapper;
    }

    /**
     * Splits dataset into train and test subsets.
     *
     * @param trainSize The proportion of the dataset to include in the train split (should be between 0 and 1).
     * @return Split with two predicates for training and testing parts.
     */
    public TrainTestSplit<K, V> split(double trainSize) {
        return split(trainSize, 1 - trainSize);
    }

    /**
     * Splits dataset into train and test subsets.
     *
     * @param trainSize The proportion of the dataset to include in the train split (should be between 0 and 1).
     * @param testSize The proportion of the dataset to include in the test split (should be a number between 0 and 1).
     * @return Split with two predicates for training and testing parts.
     */
    public TrainTestSplit<K, V> split(double trainSize, double testSize) {
        return new TrainTestSplit<>(
            new DatasetSplitFilter(mapper, 0, trainSize),
            new DatasetSplitFilter(mapper, trainSize, trainSize + testSize)
        );
    }

    /**
     * Dataset filter based on the uniform mapping and specified interval. It allows to specify a mapper that maps key-value
     * pair to a point on the segment (0, 1) and an interval inside that segment (for example (0, 0.2)). After that this
     * filter will pass all entries whose mappings lie in the specified interval.
     */
    class DatasetSplitFilter implements IgniteBiPredicate<K,V> {
        /** */
        private static final long serialVersionUID = 2247757751655582254L;

        /** Mapper used to map a key-value pair to a point on the segment (0, 1). */
        private final UniformMapper<K, V> mapper;

        /** Left point of an interval. */
        private final double from;

        /** Right point of an interval. */
        private final double to;

        /**
         * Constructs a new instance of dataset split filter.
         *
         * @param mapper Mapper used to map a key-value pair to a point on the segment (0, 1).
         * @param from Left point of an interval.
         * @param to Right point of an interval.
         */
        DatasetSplitFilter(UniformMapper<K, V> mapper, double from, double to) {
            assert from >= 0 && from <= 1 : "Point 'from' should be in interval (0, 1)";
            assert to >= 0 && to <= 1 : "Point 'to' should be in interval (0, 1)";
            assert from <= to : "Point 'from' should be less of equal to point 'to'";

            this.mapper = mapper;
            this.from = from;
            this.to = to;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(K key, V val) {
            double pnt = mapper.map(key, val);

            assert pnt >= 0 && pnt <= 1 : "Point should be in interval (0, 1)";

            return pnt >= from && pnt < to;
        }
    }
}
