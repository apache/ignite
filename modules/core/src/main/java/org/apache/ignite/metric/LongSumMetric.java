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

import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.metric.LongMetric;

/** Updatable long metric which is efficient with adding values. Calculates sum in {@link LongMetric#value()}. */
@IgniteExperimental
public interface LongSumMetric extends LongMetric {
    /**
     * Raises metric value.
     *
     * @param value An increment to add to current metric's value.
     */
    void add(long value);

    /** Increments metric value with 1L. */
    void increment();

    /** Decrements metric value with -1L. */
    void decrement();
}
