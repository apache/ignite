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

import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.selection.split.mapper.UniformMapper;

/**
 * Dataset filter based on the uniform mapping and specified interval. It allows to specify a mapper that maps key-value
 * pair to a point on the segment (0, 1) and an interval inside that segment (for example (0, 0.2)). After that this
 * filter will pass all entries whose mappings lie in the specified interval.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class DatasetSplitFilter<K, V> implements IgniteBiPredicate<K,V> {
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
    public DatasetSplitFilter(UniformMapper<K, V> mapper, double from, double to) {
        assert from >= 0 && from <= 1 : "Point 'from' should be in interval (0, 1)";
        assert to >= 0 && to <= 1: "Point 'to' should be in interval (0, 1)";
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

    /** */
    public double getFrom() {
        return from;
    }

    /** */
    public double getTo() {
        return to;
    }
}
