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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;

/**
 * Partition update counter.
 */
public class DefaultPartitionUpdateCounter implements PartitionUpdateCounter {
    /** */
    private IgniteLogger log;

    /** Counter. */
    private final AtomicLong cntr = new AtomicLong();

    /** */
    private UpdateCounterGenerator updCntrGenerator;

    /** */
    private UpdateCounterGenerator initUpdCntrGenerator;

    /**
     * @param log Logger.
     */
    DefaultPartitionUpdateCounter(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Sets init counter.
     *
     * @param lwm Low watermark.
     * @param hwm High watermark.
     * @param cnt Counter.
     */
    public void init(long lwm, long hwm, long cnt) {
        updCntrGenerator = new UpdateCounterGenerator(lwm, hwm, cnt);

        cntr.set(updCntrGenerator.lwm());

        initUpdCntrGenerator = new UpdateCounterGenerator(lwm, hwm, cnt);
    }

    /**
     * @return Initial counter value.
     */
    public long initial() {
        return initUpdCntrGenerator.lwm();
    }

    /**
     * @return Current update counter value.
     */
    public long get() {
        return cntr.get();
    }

    /**
     * Adds delta to current counter value.
     *
     * @param delta Delta.
     * @return Value before add.
     */
    public long getAndAdd(long delta) {
        assert false;

        return 0;
    }

    /**
     * @return Next update counter.
     */
    public synchronized long next() {
        long cntr = this.cntr.incrementAndGet();

        updCntrGenerator.update(cntr);

        return cntr;
    }

    /**
     * Sets value to update counter.
     *
     * @param val Values.
     */
    public synchronized void update(long val) {
        updCntrGenerator.update(val);

        cntr.set(updCntrGenerator.lwm());
    }

    /**
     * Updates counter by delta from start position.
     *
     * @param start Start.
     * @param delta Delta.
     */
    public void update(long start, long delta) {
        assert false;
    }

    /**
     * Update initial counter on logical recovery.
     * Counter will only be updated if no "holes" (missed values) present in passed sequence.
     *
     * @param cntr Counter to set.
     */
    public void updateInitial(long cntr) {
        long initCntr = initial();

        if (cntr <= initCntr) {
            if (cntr == 0) { // 0 means counter reset.
                initUpdCntrGenerator = new UpdateCounterGenerator(0, 0, 0);
            }

            return;
        }

        initUpdCntrGenerator.update(cntr);
    }

    /**
     * Flushes pending update counters closing all possible gaps.
     */
    public synchronized GridLongList finalizeUpdateCounters() {
        assert false;

        return null;
    }

    @Override public synchronized long maxUpdateCounter() {
        return updCntrGenerator.hwm();
    }

    @Override public synchronized long updateCounterGap() {
        return updCntrGenerator.gapCount();
    }
}
