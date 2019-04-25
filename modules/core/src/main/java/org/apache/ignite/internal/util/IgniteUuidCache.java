/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 *
 */
public final class IgniteUuidCache {
    /** Maximum cache size. */
    private static final int MAX = 1024;

    /** Cache. */
    private static final ConcurrentMap<UUID, UUID> cache =
        new GridBoundedConcurrentLinkedHashMap<>(MAX, 1024, 0.75f, 64, PER_SEGMENT_Q);

    /**
     * Gets cached UUID to preserve memory.
     *
     * @param id Read UUID.
     * @return Cached UUID equivalent to the read one.
     */
    public static UUID onIgniteUuidRead(UUID id) {
        UUID cached = cache.get(id);

        if (cached == null) {
            UUID old = cache.putIfAbsent(id, cached = id);

            if (old != null)
                cached = old;
        }

        return cached;
    }

    /**
     * Ensure singleton.
     */
    private IgniteUuidCache() {
        // No-op.
    }
}
