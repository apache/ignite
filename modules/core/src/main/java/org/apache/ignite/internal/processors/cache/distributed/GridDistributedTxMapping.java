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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction node mapping.
 */
public class GridDistributedTxMapping implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Mapped node. */
    @GridToStringExclude
    private ClusterNode node;

    /** Entries. */
    @GridToStringInclude
    private Collection<IgniteTxEntry> entries;

    /** Explicit lock flag. */
    private boolean explicitLock;

    /** DHT version. */
    private GridCacheVersion dhtVer;

    /** Copy on remove flag. */
    private boolean readOnly;

    /** {@code True} if this is last mapping for node. */
    private boolean last;

    /** IDs of backup nodes receiving last prepare request during this mapping. */
    private Collection<UUID> lastBackups;

    /** {@code True} if mapping is for near caches, {@code false} otherwise. */
    private boolean near;

    /** {@code True} if this is first mapping for optimistic tx on client node. */
    private boolean clientFirst;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDistributedTxMapping() {
        // No-op.
    }

    /**
     * @param node Mapped node.
     */
    public GridDistributedTxMapping(ClusterNode node) {
        this.node = node;

        entries = new GridConcurrentLinkedHashSet<>();
    }

    /**
     * @return IDs of backup nodes receiving last prepare request during this mapping.
     */
    @Nullable public Collection<UUID> lastBackups() {
        return lastBackups;
    }

    /**
     * @param lastBackups IDs of backup nodes receiving last prepare request during this mapping.
     */
    public void lastBackups(@Nullable Collection<UUID> lastBackups) {
        this.lastBackups = lastBackups;
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
     * @return {@code True} if mapping is for near caches, {@code false} otherwise.
     */
    public boolean near() {
        return near;
    }

    /**
     * @param near {@code True} if mapping is for near caches, {@code false} otherwise.
     */
    public void near(boolean near) {
        this.near = near;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * @return Entries.
     */
    public Collection<IgniteTxEntry> entries() {
        return entries;
    }

    /**
     * @param entries Mapped entries.
     * @param readOnly Flag indicating that passed in collection is read-only.
     */
    public void entries(Collection<IgniteTxEntry> entries, boolean readOnly) {
        this.entries = entries;

        // Set copy on remove flag as passed in collection is unmodifiable.
        this.readOnly = true;
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
        return F.view(entries, CU.reads());
    }

    /**
     * @return Writes.
     */
    public Collection<IgniteTxEntry> writes() {
        return F.view(entries, CU.writes());
    }

    /**
     * @param entry Adds entry.
     */
    public void add(IgniteTxEntry entry) {
        ensureModifiable();

        entries.add(entry);
    }

    /**
     * @param entry Entry to remove.
     * @return {@code True} if entry was removed.
     */
    public boolean removeEntry(IgniteTxEntry entry) {
        ensureModifiable();

        return entries.remove(entry);
    }

    /**
     * @param parts Evicts partitions from mapping.
     */
    public void evictPartitions(@Nullable int[] parts) {
        if (!F.isEmpty(parts)) {
            ensureModifiable();

            evictPartitions(parts, entries);
        }
    }

    /**
     * @param parts Partitions.
     * @param c Collection.
     */
    private void evictPartitions(int[] parts, Collection<IgniteTxEntry> c) {
        assert parts != null;

        for (Iterator<IgniteTxEntry> it = c.iterator(); it.hasNext();) {
            IgniteTxEntry e = it.next();

            GridCacheEntryEx cached = e.cached();

            if (U.containsIntArray(parts, cached.partition()))
                it.remove();
        }
    }

    /**
     * @param keys Keys to evict readers for.
     */
    public void evictReaders(@Nullable Collection<IgniteTxKey> keys) {
        if (keys == null || keys.isEmpty())
            return;

        ensureModifiable();

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
     * Copies collection of entries if it is read-only.
     */
    private void ensureModifiable() {
        if (readOnly) {
            entries = new GridConcurrentLinkedHashSet<>(entries);

            readOnly = false;
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(node);

        U.writeCollection(out, entries);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        node = (ClusterNode)in.readObject();

        entries = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedTxMapping.class, this, "node", node.id());
    }
}