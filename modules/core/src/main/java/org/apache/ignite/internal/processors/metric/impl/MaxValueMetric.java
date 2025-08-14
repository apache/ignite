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
 * Accumulates approximate maximum value statistics.
 * Calculates maximum value in last {@code timeInterval} milliseconds.
 *
 * @see AbstractIntervalMetric for implementation details.
 */
public class MaxValueMetric extends AbstractIntervalMetric {
    /**
     * @param name Name.
     * @param desc Description.
     * @param timeInterval Time interval in milliseconds.
     * @param size Values array size (number of buckets).
     */
    public MaxValueMetric(String name, @Nullable String desc, long timeInterval, int size) {
        super(name, desc, timeInterval, size);
    }

    /** {@inheritDoc} */
    @Override protected AbstractIntervalMetricImpl createImpl(long timeInterval, int size) {
        return new MaxValueMetricImpl(timeInterval, size);
    }

    /**
     * Accumulate x value to the metric.
     *
     * @param x Value to be accumulate.
     */
    public void update(long x) {
        cntr.update(x);
    }

    /**
     * Actual metric.
     */
    private static class MaxValueMetricImpl extends AbstractIntervalMetricImpl {
        /**
         * @param timeInterval Time interval.
         * @param size Buckets count.
         */
        public MaxValueMetricImpl(long timeInterval, int size) {
            super(timeInterval, size);
        }

        /** {@inheritDoc} */
        @Override protected long accumulate(long res, long val) {
            return Math.max(res, val);
        }

        /** {@inheritDoc} */
        @Override protected void accumulateBucket(int bucket, long val) {
            long oldVal;
            long val0;

            do {
                oldVal = taggedVals.get(bucket);

                val0 = (val & NO_TAG_MASK) | (oldVal & TAG_MASK);

                if (val0 <= oldVal)
                    return;
            }
            while (!taggedVals.compareAndSet(bucket, oldVal, val0));
        }
    }
}
