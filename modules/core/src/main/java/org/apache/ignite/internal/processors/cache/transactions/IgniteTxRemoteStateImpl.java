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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class IgniteTxRemoteStateImpl extends IgniteTxRemoteStateAdapter {
    /** Read set. */
    @GridToStringInclude
    protected Map<IgniteTxKey, IgniteTxEntry> readMap;

    /** Write map. */
    @GridToStringInclude
    protected Map<IgniteTxKey, IgniteTxEntry> writeMap;

    /**
     * @param readMap Read map.
     * @param writeMap Write map.
     */
    public IgniteTxRemoteStateImpl(Map<IgniteTxKey, IgniteTxEntry> readMap,
        Map<IgniteTxKey, IgniteTxEntry> writeMap) {
        this.readMap = readMap;
        this.writeMap = writeMap;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        IgniteTxEntry e = writeMap == null ? null : writeMap.get(key);

        if (e == null)
            e = readMap == null ? null : readMap.get(key);

        return e;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return writeMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return readMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        return writeMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return writeMap.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return readMap.values();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return writeMap;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return readMap;
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return readMap.isEmpty() && writeMap.isEmpty();
    }

    /** {@inheritDoc} */
    public void addWriteEntry(IgniteTxKey key, IgniteTxEntry e) {
        writeMap.put(key, e);
    }

    /** {@inheritDoc} */
    public void clearEntry(IgniteTxKey key) {
        readMap.remove(key);
        writeMap.remove(key);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return F.concat(false, writeEntries(), readEntries());
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry singleWrite() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void invalidPartition(int part) {
        if (writeMap != null) {
            for (Iterator<IgniteTxEntry> it = writeMap.values().iterator(); it.hasNext();) {
                IgniteTxEntry e = it.next();

                GridCacheContext cacheCtx = e.context();

                GridCacheEntryEx cached = e.cached();

                if (cached != null) {
                    if (cached.partition() == part)
                        it.remove();
                }
                else if (cacheCtx.affinity().partition(e.key()) == part)
                    it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(IgniteTxRemoteStateImpl.class, this);
    }
}
