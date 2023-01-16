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

import javax.cache.management.CacheStatisticsMXBean;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

/**
 * Implementation of {@link CacheStatisticsMXBean} only to support JCache specification. An echanced statistics is
 * available through {@link JmxMetricExporterSpi} with "name=cache.{cache_name}".
 *
 * @see ReadOnlyMetricManager
 * @see ReadOnlyMetricRegistry
 * @see JmxMetricExporterSpi
 * @see MetricExporterSpi
 */
public class CacheStatisticsMXBeanImpl implements CacheStatisticsMXBean {
    /** Cache. */
    private CacheMetricsImpl metrics;

    /**
     * Creates MBean;
     *
     * @param metrics Cache metrics.
     */
    public CacheStatisticsMXBeanImpl(CacheMetricsImpl metrics) {
        assert metrics != null;

        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        metrics.clear();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return metrics.getCacheHits();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        return metrics.getCacheHitPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return metrics.getCacheMisses();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        return metrics.getCacheMissPercentage();
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return metrics.getCacheGets();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return metrics.getCachePuts();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return metrics.getCacheRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return metrics.getCacheEvictions();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        return metrics.getAverageGetTime();
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        return metrics.getAveragePutTime();
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        return metrics.getAverageRemoveTime();
    }
}
