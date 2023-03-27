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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteTxRemoteSingleStateImpl extends IgniteTxRemoteStateAdapter {
    /** */
    private IgniteTxEntry entry;

    /** {@inheritDoc} */
    @Override public void unwindEvicts(GridCacheSharedContext cctx) {
        if (entry == null)
            return;

        GridCacheContext ctx = cctx.cacheContext(entry.cacheId());

        if (ctx != null)
            CU.unwindEvicts(ctx);
    }

    /** {@inheritDoc} */
    @Override public void addWriteEntry(IgniteTxKey key, IgniteTxEntry e) {
        entry = e;
    }

    /** {@inheritDoc} */
    @Override public void clearEntry(IgniteTxKey key) {
        if (entry != null && entry.txKey().equals(key))
            entry = null;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        if (entry != null && entry.txKey().equals(key))
            return entry;

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return entry != null && entry.txKey().equals(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        return entry != null ? Collections.singleton(entry.txKey()) : Collections.<IgniteTxKey>emptySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return entry != null ? Collections.singletonList(entry) : Collections.<IgniteTxEntry>emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return entry != null ? F.asMap(entry.txKey(), entry) :
            Collections.<IgniteTxKey, IgniteTxEntry>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return entry == null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return entry != null ? Collections.singletonList(entry) : Collections.<IgniteTxEntry>emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTxEntry singleWrite() {
        return entry;
    }

    /** {@inheritDoc} */
    @Override public void invalidPartition(int cacheId, int part, GridCacheVersion ver) {
        if (entry != null && entry.context().affinity().partition(entry.key()) == part)
            entry = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxRemoteSingleStateImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx) {
        if (entry == null)
            return null;

        CacheStoreManager store = entry.context().store();

        if (store.configured()
            && store.isLocal()) { // Only local stores take part at tx on backup node.
            return Collections.singleton(store);
        }

        return null;
    }
}
