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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Statement cache. LRU eviction policy is used. Not thread-safe.
 */
class H2StatementCache {
    /**
     * Statement meta information.
     */
    static class StatementMeta {
        /** */
        private static final AtomicInteger metaIdGenerator = new AtomicInteger();

        /** */
        static final int INVOLVED_CACHES = metaIdGenerator.getAndIncrement();

        /** */
        private final PreparedStatement stmt;
        /** */
        private Object[] meta = null;

        /**
         * @param stmt Statement to bind with meta.
         */
        StatementMeta(PreparedStatement stmt) {
            this.stmt = stmt;
        }

        /**
         * Gets meta for given id.
         *
         * @param id Meta id.
         * @param <T> Meta object type.
         * @return Meta object.
         */
        @SuppressWarnings("unchecked")
        @Nullable <T> T meta(int id) {
            return meta != null && id < meta.length ? (T) meta[id] : null;
        }

        /**
         * Puts meta for given id.
         *
         * @param id Meta id.
         * @param metaObj Meta object.
         */
        void putMeta(int id, Object metaObj) {
            if (meta == null || id >= meta.length)
                meta = new Object[id + 1];

            meta[id] = metaObj;
        }
    }

    /** Last usage. */
    private volatile long lastUsage;

    /** */
    private final LinkedHashMap<H2CachedStatementKey, StatementMeta> lruStmtCache;

    /**
     * @param size Maximum number of statements this cache can store.
     */
    H2StatementCache(int size) {
        lruStmtCache = new LinkedHashMap<H2CachedStatementKey, StatementMeta>(size, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<H2CachedStatementKey, StatementMeta> eldest) {
                boolean rmv = size() > size;

                if (rmv) {
                    StatementMeta stmtWithMeta = eldest.getValue();

                    U.closeQuiet(stmtWithMeta.stmt);
                }

                return rmv;
            }
        };
    }

    /**
     * Caches a statement.
     *
     * @param key Key associated with statement.
     * @param stmt Statement which will be cached.
     */
    void put(H2CachedStatementKey key, PreparedStatement stmt) {
        lruStmtCache.put(key, new StatementMeta(stmt));
    }

    /**
     * Retrieves cached statement.
     *
     * @param key Key for a statement.
     * @return Statement associated with a key.
     */
    @Nullable PreparedStatement get(H2CachedStatementKey key) {
        StatementMeta stmtWitMeta = lruStmtCache.get(key);
        return stmtWitMeta != null ? stmtWitMeta.stmt : null;
    }

    /**
     * Retrieves cached statement with meta.
     *
     * @param key Key for a statement.
     * @return Statement meta associated with a key.
     */
    @Nullable StatementMeta getStatementMeta(H2CachedStatementKey key) {
        return lruStmtCache.get(key);
    }

    /**
     * The timestamp of the last usage of the cache.
     *
     * @return last usage timestamp
     */
    long lastUsage() {
        return lastUsage;
    }

    /**
     * Updates the {@link #lastUsage} timestamp by current time.
     */
    void updateLastUsage() {
        lastUsage = U.currentTimeMillis();
    }

    /**
     * Remove statement for given schema and SQL.
     * @param schemaName Schema name.
     * @param sql SQL statement.
     * @return Cached {@link PreparedStatement}, or {@code null} if none found.
     */
    @Nullable
    StatementMeta remove(String schemaName, String sql) {
        return lruStmtCache.remove(new H2CachedStatementKey(schemaName, sql));
    }
}
