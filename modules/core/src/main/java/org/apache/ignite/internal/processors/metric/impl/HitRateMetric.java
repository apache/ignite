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
package org.apache.ignite.internal.processors.metric.impl;

import org.jetbrains.annotations.Nullable;

/**
 * Accumulates approximate hit rate statistics.
 * Calculates number of hits in last {@code rateTimeInterval} milliseconds.
 *
 * @see AbstractIntervalMetric for implementation details.
 */
public class HitRateMetric extends AbstractIntervalMetric {
    /**
     * @param name Name.
     * @param desc Description.
     * @param rateTimeInterval Rate time interval in milliseconds.
     * @param size Counters array size.
     */
    public HitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        super(name, desc, rateTimeInterval, size);
    }

    /** {@inheritDoc} */
    @Override protected AbstractIntervalMetricImpl createImpl(long timeInterval, int size) {
        return new HitRateMetricImpl(timeInterval, size);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        cntr.update(x);
    }

    /** Adds 1 to the metric. */
    public void increment() {
        add(1);
    }

    /**
     * Actual metric.
     */
    private static class HitRateMetricImpl extends AbstractIntervalMetricImpl {
        /**
         * @param timeInterval Time interval.
         * @param size Buckets count.
         */
        public HitRateMetricImpl(long timeInterval, int size) {
            super(timeInterval, size);
        }

        /** {@inheritDoc} */
        @Override protected long accumulate(long res, long val) {
            return res + val;
        }

        /** {@inheritDoc} */
        @Override protected void accumulateBucket(int bucket, long val) {
            taggedVals.addAndGet(bucket, val);
        }
    }
}
