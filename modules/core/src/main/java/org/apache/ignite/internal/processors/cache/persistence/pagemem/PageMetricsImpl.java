/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.jetbrains.annotations.Nullable;

/** */
public class PageMetricsImpl implements PageMetrics {
    /** Total pages. */
    private final LongAdderMetric totalPages;

    /** Index pages in memory. */
    private final LongAdderMetric idxPages;

    /** */
    private PageMetricsImpl(
        MetricRegistry metricRegistry,
        @Nullable LongAdderWithDelegateMetric.Delegate totalPagesCb,
        @Nullable LongAdderWithDelegateMetric.Delegate idxPagesCb
    ) {
        totalPages = createMetricWithOptionalDelegate(
            metricRegistry, "TotalAllocatedPages", "Total allocated pages.", totalPagesCb
        );

        idxPages = createMetricWithOptionalDelegate(
            metricRegistry, "InMemoryIndexPages", "Amount of index pages loaded into memory.", idxPagesCb
        );
    }

    /**
     * Builder for {@link PageMetricsImpl} instances.
     */
    public static final class Builder {
        /** Metric registry. */
        private final MetricRegistry metricRegistry;

        /** Total pages callback. */
        private LongAdderWithDelegateMetric.Delegate totalPagesCb;

        /** Index pages callback. */
        private LongAdderWithDelegateMetric.Delegate idxPagesCb;

        /**
         * @param metricRegistry Metric registry.
         */
        Builder(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
        }

        /**
         * @param cb Callback.
         */
        public Builder totalPagesCallback(LongAdderWithDelegateMetric.Delegate cb) {
            totalPagesCb = cb;
            return this;
        }

        /**
         * @param cb Callback.
         */
        public Builder indexPagesCallback(LongAdderWithDelegateMetric.Delegate cb) {
            idxPagesCb = cb;
            return this;
        }

        /** */
        public PageMetricsImpl build() {
            return new PageMetricsImpl(
                metricRegistry,
                totalPagesCb,
                idxPagesCb
            );
        }
    }

    /**
     * @param metricRegistry Metric registry.
     */
    public static Builder builder(MetricRegistry metricRegistry) {
        return new Builder(metricRegistry);
    }

    /**
     * @param metricRegistry Metric registry.
     * @param name Name.
     * @param desc Description.
     * @param delegate Delegate.
     */
    private static LongAdderMetric createMetricWithOptionalDelegate(
        MetricRegistry metricRegistry,
        String name,
        String desc,
        @Nullable LongAdderWithDelegateMetric.Delegate delegate
    ) {
        return delegate == null ?
            metricRegistry.longAdderMetric(name, desc) :
            metricRegistry.longAdderMetric(name, delegate, desc);
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric totalPages() {
        return totalPages;
    }


    /** {@inheritDoc} */
    @Override public LongAdderMetric indexPages() {
        return idxPages;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        totalPages.reset();
        idxPages.reset();
    }
}
