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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/**
 * Class to pass to remote nodes transaction changes.
 *
 * @see TransactionConfiguration#setTxAwareQueriesEnabled(boolean)
 * @see ExecutionContext#transactionChanges(Collection)
 * @see ExecutionContext#transactionChanges(int, int[], Function, Comparator)
 * @see QueryStartRequest#queryTransactionEntries()
 */
public class QueryTxEntry implements CalciteMessage {
    /** Cache id. */
    @Order(0)
    private int cacheId;

    /** Entry key. */
    @Order(1)
    private KeyCacheObject key;

    /** Entry value. */
    @Order(value = 2, method = "value")
    private CacheObject val;

    /** Entry version. */
    @Order(value = 3, method = "version")
    private GridCacheVersion ver;

    /**
     * Empty constructor.
     */
    public QueryTxEntry() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     */
    public QueryTxEntry(int cacheId, KeyCacheObject key, CacheObject val, GridCacheVersion ver) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.ver = ver;
    }

    /** @return Cache id. */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache id.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /** @return Entry key. */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /** @return Entry value. */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void value(CacheObject val) {
        this.val = val;
    }

    /** @return Entry version. */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver New entry version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /** */
    public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        CacheObjectContext coctx = ctx.cacheContext(cacheId).cacheObjectContext();

        key.prepareMarshal(coctx);

        if (val != null)
            val.prepareMarshal(coctx);
    }

    /** */
    public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        CacheObjectContext coctx = ctx.cacheContext(cacheId).cacheObjectContext();

        key.finishUnmarshal(coctx, ldr);

        if (val != null)
            val.finishUnmarshal(coctx, ldr);

    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_TX_ENTRY;
    }
}
