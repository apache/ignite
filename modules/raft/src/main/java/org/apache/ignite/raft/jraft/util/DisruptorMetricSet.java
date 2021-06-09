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
package org.apache.ignite.raft.jraft.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.lmax.disruptor.RingBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Disruptor metric set including buffer-size, remaining-capacity etc.
 */
public final class DisruptorMetricSet implements MetricSet {

    private final RingBuffer<?> ringBuffer;

    public DisruptorMetricSet(RingBuffer<?> ringBuffer) {
        super();
        this.ringBuffer = ringBuffer;
    }

    /**
     * Return disruptor metrics
     *
     * @return disruptor metrics map
     */
    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<>();
        gauges.put("buffer-size", (Gauge<Integer>) this.ringBuffer::getBufferSize);
        gauges.put("remaining-capacity", (Gauge<Long>) this.ringBuffer::remainingCapacity);
        return gauges;
    }
}
