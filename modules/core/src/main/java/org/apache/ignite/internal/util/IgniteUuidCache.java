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

package org.apache.ignite.internal.util;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public final class IgniteUuidCache {
    /** Maximum cache size. */
    private static final int MAX = 1024;

    /** Cache. */
    private static final ConcurrentMap<UUID, UUID> cache =
        new GridBoundedConcurrentLinkedHashMap<>(MAX, 1024, 0.75f, 64);

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