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

import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Per-thread cache of {@link StringBuilder} instances.
 */
public final class GridStringBuilderFactory {
    /** Cache string builders per thread for better performance. */
    private static ThreadLocal<CachedBuilder> builders = new ThreadLocal<CachedBuilder>() {
        @Override protected CachedBuilder initialValue() {
            return new CachedBuilder();
        }
    };

    /**
     * Acquires a cached instance of {@link StringBuilder} if
     * current thread is not using it yet. Otherwise a new
     * instance of string builder is returned.
     *
     * @return Cached instance of {@link StringBuilder}.
     */
    public static SB acquire() {
        return builders.get().acquire();
    }

    /**
     * Releases {@link StringBuilder} back to cache.
     *
     * @param builder String builder to release.
     */
    public static void release(SB builder) {
        builders.get().release(builder);
    }

    /**
     * No-op constructor to ensure singleton.
     */
    private GridStringBuilderFactory() {
        /* No-op. */
    }

    /**
     * Cached builder.
     */
    private static class CachedBuilder {
        /** Cached builder. */
        private SB builder = new SB();

        /** {@code True} if already used by a thread. */
        private boolean used;

        /**
         * @return The cached builder.
         */
        public SB acquire() {
            // If cached instance is already used, then we don't optimize.
            // Simply return a new StringBuilder in such case.
            if (used)
                return new SB();

            used = true;

            return builder;
        }

        /**
         * Releases builder for reuse.
         *
         * @param builder Builder to release.
         */
        @SuppressWarnings({"ObjectEquality"})
        public void release(SB builder) {
            if (this.builder == builder) {
                builder.setLength(0);

                used = false;
            }
        }
    }
}