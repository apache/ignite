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

package org.apache.ignite.internal.processors.cluster;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public final class CacheMetricsMessage implements Message {
    /** */
    public static final short TYPE_CODE = 134;

    /** @see CacheMetricsSnapshot */
    @Order(0)
    private Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots;

    /** */
    public CacheMetricsMessage() {
        // No-op.
    }

    /** */
    public CacheMetricsMessage(Map<Integer, ? extends CacheMetrics> cacheMetrics) {
        assert cacheMetrics != null;

        cacheMetricsSnapshots = wrap(cacheMetrics);
    }

    /** */
    public static Map<Integer, CacheMetricsSnapshot> wrap(Map<Integer, ? extends CacheMetrics> cacheMetrics) {
        if (F.isEmpty(cacheMetrics))
            return Collections.emptyMap();

        return cacheMetrics.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), CacheMetricsSnapshot.of(e.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** */
    public Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots() {
        return cacheMetricsSnapshots;
    }

    /** */
    public void cacheMetricsSnapshots(Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots) {
        this.cacheMetricsSnapshots = cacheMetricsSnapshots;
    }

    /** */
    public Map<Integer, CacheMetrics> cacheMetrics() {
        if (F.isEmpty(cacheMetricsSnapshots))
            return Collections.emptyMap();

        return cacheMetricsSnapshots.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), (CacheMetrics)e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsMessage.class, this);
    }
}
