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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Cache metrics wrapper message. */
public final class CacheMetricsMessage implements Message {
    /** Message type code. */
    public static final short TYPE_CODE = 136;

    /** Map of cache metrics snapshots. */
    @Order(0)
    private Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots;

    /** Constructor. */
    public CacheMetricsMessage() {
        // No-op.
    }

    /** Constructor. */
    public CacheMetricsMessage(Map<Integer, ? extends CacheMetrics> cacheMetrics) {
        assert cacheMetrics != null;

        if (F.isEmpty(cacheMetrics))
            cacheMetricsSnapshots = Collections.emptyMap();
        else {
            cacheMetricsSnapshots = new HashMap<>(cacheMetrics.size(), 1.0f);

            cacheMetrics.forEach((id, m) -> cacheMetricsSnapshots.put(id, CacheMetricsSnapshot.of(m)));
        }
    }

    /** @return Map of cache metrics snapshots. */
    public Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots() {
        return cacheMetricsSnapshots;
    }

    /** @param cacheMetricsSnapshots Map of cache metrics snapshots. */
    public void cacheMetricsSnapshots(Map<Integer, CacheMetricsSnapshot> cacheMetricsSnapshots) {
        this.cacheMetricsSnapshots = cacheMetricsSnapshots;
    }

    /** @return Map of cache metrics. */
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
