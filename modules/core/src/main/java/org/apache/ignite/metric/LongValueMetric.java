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

package org.apache.ignite.metric;

import org.apache.ignite.spi.metric.LongMetric;

/**
 * A metric exposing a long value which can be set externally.
 */
public interface LongValueMetric extends LongMetric {
    /**
     * Adds a value to the metric.
     *
     * @param x Value to be added.
     */
    void add(long x);

    /** Adds 1L to the metric. */
    default void increment() {
        add(1L);
    }

    /** Adds -1L to the metric. */
    default void decrement() {
        add(-1);
    }

    /**
     * Sets a value to the metric.
     *
     * @param x Value to be set.
     */
    void set(long x);
}
