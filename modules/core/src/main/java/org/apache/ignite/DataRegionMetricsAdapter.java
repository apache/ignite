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
package org.apache.ignite;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * Converter class from {@link DataRegionMetrics} to legacy {@link MemoryMetrics}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public class DataRegionMetricsAdapter implements MemoryMetrics {
    /** Delegate. */
    private final DataRegionMetrics delegate;

    /**
     * @param delegate Delegate.
     */
    private DataRegionMetricsAdapter(DataRegionMetrics delegate) {
        this.delegate = delegate;
    }

    /**
     * Converts collection of {@link DataRegionMetrics} into collection of legacy {@link MemoryMetrics}.
     *
     * @param dataRegionMetrics Data region metrics collection.
     */
    public static Collection<MemoryMetrics> collectionOf(Collection<DataRegionMetrics> dataRegionMetrics) {
        if (dataRegionMetrics == null)
            return null;

        Collection<MemoryMetrics> res = new ArrayList<>();

        for (DataRegionMetrics d : dataRegionMetrics)
            res.add(new DataRegionMetricsAdapter(d));

        return res;
    }

    /**
     * @param delegate DataRegionMetrics.
     * @return Wrapped {@link DataRegionMetrics} that implements {@link MemoryMetrics}.
     * Null value is not wrapped and returned as is.
     */
    public static DataRegionMetricsAdapter valueOf(DataRegionMetrics delegate) {
        return delegate == null ? null : new DataRegionMetricsAdapter(delegate);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.getName();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return delegate.getTotalAllocatedPages();
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        return delegate.getAllocationRate();
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return delegate.getEvictionRate();
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        return delegate.getLargeEntriesPagesPercentage();
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        return delegate.getPagesFillFactor();
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return delegate.getDirtyPages();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        return delegate.getPagesReplaceRate();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        return delegate.getPhysicalMemoryPages();
    }
}
