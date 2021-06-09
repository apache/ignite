/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.core;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Node metrics
 */
public class NodeMetrics {

    private final MetricRegistry metrics;

    public NodeMetrics(final boolean enableMetrics) {
        if (enableMetrics) {
            this.metrics = new MetricRegistry();
        }
        else {
            this.metrics = null;
        }
    }

    /**
     * Retrieve the metrics map, returns empty map if it is disabled.
     *
     * @return metrics map
     */
    public Map<String, Metric> getMetrics() {
        if (this.metrics != null) {
            return this.metrics.getMetrics();
        }
        return Collections.emptyMap();
    }

    /**
     * Retrieve the metrics registry, return null if is is disabled.
     *
     * @return metrics registry
     */
    public MetricRegistry getMetricRegistry() {
        return this.metrics;
    }

    /**
     * Whether metric is enabled.
     *
     * @return true if metric is enabled
     */
    public boolean isEnabled() {
        return this.metrics != null;
    }

    /**
     * Records operation times.
     *
     * @param key key of operation
     * @param times times of operation
     */
    public void recordTimes(final String key, final long times) {
        if (this.metrics != null) {
            this.metrics.counter(key).inc(times);
        }
    }

    /**
     * Records operation batch size.
     *
     * @param key key of operation
     * @param size size of operation
     */
    public void recordSize(final String key, final long size) {
        if (this.metrics != null) {
            this.metrics.histogram(key).update(size);
        }
    }

    /**
     * Records operation latency.
     *
     * @param key key of operation
     * @param duration duration of operation
     */
    public void recordLatency(final String key, final long duration) {
        if (this.metrics != null) {
            this.metrics.timer(key).update(duration, TimeUnit.MILLISECONDS);
        }
    }
}
