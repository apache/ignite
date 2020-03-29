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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction node mapping.
 */
public class GridDistributedTxMapping {
    /** */
    private static final AtomicReferenceFieldUpdater<GridDistributedTxMapping, Set> BACKUPS_FIELD_UPDATER
        = AtomicReferenceFieldUpdater.newUpdater(GridDistributedTxMapping.class, Set.class, "backups");

    /** Mapped node. */
    @GridToStringExclude
    private ClusterNode primary;

    /** Mapped backup nodes. */
    private volatile Set<UUID> backups;

    /** Entries. */
    @GridToStringInclude
    private final Collection<IgniteTxEntry> entries;

    /** Explicit lock flag. */
    private boolean explicitLock;

    /** Query update flag. */
    private boolean queryUpdate;

    /** DHT version. */
    private GridCacheVersion dhtVer;

    /** {@code True} if this is last mapping for node. */
    private boolean last;

    /** Near cache entries count. */
    private int nearEntries;

    /** {@code True} if this is first mapping for optimistic tx on client node. */
    private boolean clientFirst;

    /**
     * @param primary Primary node.
     */
    public GridDistributedTxMapping(ClusterNode primary) {
        this.primary = primary;

        entries = new LinkedHashSet<>();
    }

    /**
     * @return {@code True} if this is last mapping for node.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last If {@code True} this is last mapping for node.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @return {@code True} if this is first mapping for optimistic tx on client node.
     */
    public boolean clientFirst() {
        return clientFirst;
    }

    /**
     * @param clientFirst {@code True} if this is first mapping for optimistic tx on client node.
     */
    public void clientFirst(boolean clientFirst) {
        this.clientFirst = clientFirst;
    }

    /**
     * @return {@code True} if has colocated cache entries.
     */
    public boolean hasColocatedCacheEntries() {
        return entries.size() > nearEntries;
    }

    /**
     * @return {@code True} if has near cache entries.
     */
    public boolean hasNearCacheEntries() {
        return nearEntries > 0;
    }

    /**
     * @return Node.
     */
    public ClusterNode primary() {
        return primary;
    }

    /**
     * @return Entries.
     */
    public Collection<IgniteTxEntry> entries() {
        return entries;
    }

    /**
     * @return Near cache entries.
     */
    @Nullable public Collection<IgniteTxEntry> nearCacheEntries() {
        assert nearEntries > 0;

        return F.view(entries, CU.FILTER_NEAR_CACHE_ENTRY);
    }

    /**
     * @return {@code True} if mapping was created for a query update.
     */
    public boolean queryUpdate() {
        return queryUpdate;
    }

    /**
     * Sets query update flag to {@code true}.
     */
    public void markQueryUpdate() {
        queryUpdate = true;
    }

    /**
     * @return {@code True} if lock is explicit.
     */
    public boolean explicitLock() {
        return explicitLock;
    }

    /**
     * Sets explicit flag to {@code true}.
     */
    public void markExplicitLock() {
        explicitLock = true;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer DHT version.
     * @param writeVer DHT writeVersion.
     */
    public void dhtVersion(GridCacheVersion dhtVer, GridCacheVersion writeVer) {
        this.dhtVer = dhtVer;

        for (IgniteTxEntry e : entries)
            e.dhtVersion(writeVer);
    }

    /**
     * @return Reads.
     */
    public Collection<IgniteTxEntry> reads() {
        return F.view(entries, CU.READ_FILTER);
    }

    /**
     * @return Writes.
     */
    public Collection<IgniteTxEntry> writes() {
        return F.view(entries, CU.WRITE_FILTER);
    }

    /**
     * @return Near cache reads.
     */
    public Collection<IgniteTxEntry> nearEntriesReads() {
        assert hasNearCacheEntries();

        return F.view(entries, CU.READ_FILTER_NEAR);
    }

    /**
     * @return Near cache writes.
     */
    public Collection<IgniteTxEntry> nearEntriesWrites() {
        assert hasNearCacheEntries();

        return F.view(entries, CU.WRITE_FILTER_NEAR);
    }

    /**
     * @return Colocated cache reads.
     */
    public Collection<IgniteTxEntry> colocatedEntriesReads() {
        assert hasColocatedCacheEntries();

        return F.view(entries, CU.READ_FILTER_COLOCATED);
    }

    /**
     * @return Colocated cache writes.
     */
    public Collection<IgniteTxEntry> colocatedEntriesWrites() {
        assert hasColocatedCacheEntries();

        return F.view(entries, CU.WRITE_FILTER_COLOCATED);
    }

    /**
     * @param entry Adds entry.
     */
    public void add(IgniteTxEntry entry) {
        if (entries.add(entry) && entry.context().isNear())
            nearEntries++;
    }

    /**
     * @param entry Entry to remove.
     * @return {@code True} if entry was removed.
     */
    public boolean removeEntry(IgniteTxEntry entry) {
        return entries.remove(entry);
    }

    /**
     * @param keys Keys to evict readers for.
     */
    public void evictReaders(@Nullable Collection<IgniteTxKey> keys) {
        if (keys == null || keys.isEmpty())
            return;

        evictReaders(keys, entries);
    }

    /**
     * @param keys Keys to evict readers for.
     * @param entries Entries to check.
     */
    private void evictReaders(Collection<IgniteTxKey> keys, @Nullable Collection<IgniteTxEntry> entries) {
        if (entries == null || entries.isEmpty())
            return;

        for (Iterator<IgniteTxEntry> it = entries.iterator(); it.hasNext();) {
            IgniteTxEntry entry = it.next();

            if (keys.contains(entry.txKey()))
                it.remove();
        }
    }

    /**
     * Whether empty or not.
     *
     * @return Empty or not.
     */
    public boolean empty() {
        return entries.isEmpty();
    }

    /**
     * @param newBackups Backups to be added to this mapping.
     */
    public void addBackups(Collection<UUID> newBackups) {
        if (newBackups == null)
            return;

        if (backups == null)
            BACKUPS_FIELD_UPDATER.compareAndSet(this, null, Collections.newSetFromMap(new ConcurrentHashMap<>()));

        backups.addAll(newBackups);
    }

    /**
     * @return Mapped backup nodes.
     */
    public Set<UUID> backups() {
        return backups != null ? backups : Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedTxMapping.class, this, "node", primary.id());
    }
}
