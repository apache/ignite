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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.GridLongList;

/**
 * Partition update counter.
 */
public interface PartitionUpdateCounter {
    /**
     * Init update counter.
     *
     * @param lwm Low watermark.
     * @param hwm High watermark.
     * @param cnt Gaps counter.
     */
    public void init(long lwm, long hwm, long cnt);

    /**
     * @return Initial counter value.
     */
    public long initial();

    /**
     * @return Current update counter value.
     */
    public long get();

    /**
     * Adds delta to current counter value.
     *
     * @param delta Delta.
     * @return Value before add.
     */
    public long getAndAdd(long delta);

    /**
     * @return Next update counter.
     */
    public long next();

    /**
     * Sets value to update counter.
     *
     * @param val Values.
     */
    public void update(long val);

    /**
     * Updates counter by delta from start position.
     *
     * @param start Start.
     * @param delta Delta.
     */
    public void update(long start, long delta);

    /**
     * Update initial counter on logical recovery.
     * Counter will only be updated if no "holes" (missed values) present in passed sequence.
     *
     * @param cntr Counter to set.
     */
    public void updateInitial(long cntr);

    /**
     * Flushes pending update counters closing all possible gaps.
     */
    public GridLongList finalizeUpdateCounters();

    public long maxUpdateCounter();

    public long updateCounterGap();
}
