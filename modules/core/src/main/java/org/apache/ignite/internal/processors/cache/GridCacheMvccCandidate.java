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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.DHT_LOCAL;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.LOCAL;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.NEAR_LOCAL;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.OWNER;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.READY;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.REENTRY;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.REMOVED;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.SINGLE_IMPLICIT;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.TX;
import static org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate.Mask.USED;

/**
 * Lock candidate.
 */
public class GridCacheMvccCandidate implements Externalizable,
    Comparable<GridCacheMvccCandidate>, CacheLockCandidates {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID generator. */
    private static final AtomicLong IDGEN = new AtomicLong();

    /** Locking node ID. */
    @GridToStringInclude
    private UUID nodeId;

    /** Lock version. */
    @GridToStringInclude
    private GridCacheVersion ver;

    /** Thread ID. */
    @GridToStringInclude
    private long threadId;

    /** Use flags approach to preserve space. */
    @GridToStringExclude
    private short flags;

    /** ID. */
    private long id;

    /** Topology version. */
    @SuppressWarnings( {"TransientFieldNotInitialized"})
    @GridToStringInclude
    private transient volatile AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

    /** Linked reentry. */
    private GridCacheMvccCandidate reentry;

    /** Previous lock for the thread. */
    @GridToStringExclude
    private transient volatile GridCacheMvccCandidate prev;

    /** Next lock for the thread. */
    @GridToStringExclude
    private transient volatile GridCacheMvccCandidate next;

    /** Parent entry. */
    @GridToStringExclude
    private transient GridCacheEntryEx parent;

    /** Alternate node ID specifying additional node involved in this lock. */
    private transient volatile UUID otherNodeId;

    /** Other lock version (near version vs dht version). */
    private transient GridCacheVersion otherVer;

    /** Mapped DHT node IDs. */
    @GridToStringInclude
    private transient volatile Collection<ClusterNode> mappedDhtNodes;

    /** Mapped near node IDs. */
    @GridToStringInclude
    private transient volatile Collection<ClusterNode> mappedNearNodes;

    /** Owned lock version by the moment this candidate was added. */
    @GridToStringInclude
    private transient volatile GridCacheVersion ownerVer;

    /** */
    private GridCacheVersion serOrder;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheMvccCandidate() {
        /* No-op. */
    }

    /**
     * @param parent Parent entry.
     * @param nodeId Requesting node ID.
     * @param otherNodeId Near node ID.
     * @param otherVer Other version.
     * @param threadId Requesting thread ID.
     * @param ver Cache version.
     * @param loc {@code True} if the lock is local.
     * @param reentry {@code True} if candidate is for reentry.
     * @param tx Transaction flag.
     * @param singleImplicit Single-key-implicit-transaction flag.
     * @param nearLoc Near-local flag.
     * @param dhtLoc DHT local flag.
     * @param serOrder Version for serializable transactions ordering.
     * @param read Read lock flag.
     */
    public GridCacheMvccCandidate(
        GridCacheEntryEx parent,
        UUID nodeId,
        @Nullable UUID otherNodeId,
        @Nullable GridCacheVersion otherVer,
        long threadId,
        GridCacheVersion ver,
        boolean loc,
        boolean reentry,
        boolean tx,
        boolean singleImplicit,
        boolean nearLoc,
        boolean dhtLoc,
        @Nullable GridCacheVersion serOrder,
        boolean read
    ) {
        assert nodeId != null;
        assert ver != null;
        assert parent != null;

        this.parent = parent;
        this.nodeId = nodeId;
        this.otherNodeId = otherNodeId;
        this.otherVer = otherVer;
        this.threadId = threadId;
        this.ver = ver;
        this.serOrder = serOrder;

        mask(LOCAL, loc);
        mask(REENTRY, reentry);
        mask(TX, tx);
        mask(SINGLE_IMPLICIT, singleImplicit);
        mask(NEAR_LOCAL, nearLoc);
        mask(DHT_LOCAL, dhtLoc);
        mask(READ, read);

        id = IDGEN.incrementAndGet();
    }

    /**
     * Sets mask value.
     *
     * @param mask Mask.
     * @param on Flag.
     */
    private void mask(Mask mask, boolean on) {
        flags = mask.set(flags, on);
    }

    /**
     * @return Flags.
     */
    public short flags() {
        return flags;
    }

    /**
     * @return Parent entry.
     */
    @SuppressWarnings({"unchecked"})
    public <V> GridCacheEntryEx parent() {
        return parent;
    }

    /**
     * @return Topology for which this lock was acquired.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Reentry candidate.
     */
    public GridCacheMvccCandidate reenter() {
        GridCacheMvccCandidate old = reentry;

        GridCacheMvccCandidate reentry = new GridCacheMvccCandidate(
            parent,
            nodeId,
            otherNodeId,
            otherVer,
            threadId,
            ver,
            local(),
            /*reentry*/true,
            tx(),
            singleImplicit(),
            nearLocal(),
            dhtLocal(),
            serializableOrder(),
            read());

        reentry.topVer = topVer;

        if (old != null)
            reentry.reentry = old;

        this.reentry = reentry;

        return reentry;
    }

    /**
     * @return Removed reentry candidate or {@code null}.
     */
    @Nullable public GridCacheMvccCandidate unenter() {
        if (reentry != null) {
            GridCacheMvccCandidate old = reentry;

            // Link to next.
            reentry = reentry.reentry;

            return old;
        }

        return null;
    }

    /**
     * @param parent Sets locks parent entry.
     */
    public void parent(GridCacheEntryEx parent) {
        assert parent != null;

        this.parent = parent;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Near or DHT node ID.
     */
    public UUID otherNodeId() {
        return otherNodeId;
    }

    /**
     * @param otherNodeId Near or DHT node ID.
     */
    public void otherNodeId(UUID otherNodeId) {
        this.otherNodeId = otherNodeId;
    }

    /**
     * @return Mapped node IDs.
     */
    public Collection<ClusterNode> mappedDhtNodes() {
        return mappedDhtNodes;
    }

    /**
     * @return Mapped node IDs.
     */
    public Collection<ClusterNode> mappedNearNodes() {
        return mappedNearNodes;
    }

    /**
     * @param mappedDhtNodes Mapped DHT node IDs.
     */
    public void mappedNodeIds(Collection<ClusterNode> mappedDhtNodes, Collection<ClusterNode> mappedNearNodes) {
        this.mappedDhtNodes = mappedDhtNodes;
        this.mappedNearNodes = mappedNearNodes;
    }

    /**
     * @param node Node to remove.
     */
    public void removeMappedNode(ClusterNode node) {
        if (mappedDhtNodes.contains(node))
            mappedDhtNodes = new ArrayList<>(F.view(mappedDhtNodes, F.notEqualTo(node)));

        if (mappedNearNodes != null && mappedNearNodes.contains(node))
            mappedNearNodes = new ArrayList<>(F.view(mappedNearNodes, F.notEqualTo(node)));
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion otherVersion() {
        return otherVer;
    }

    /**
     * Sets mapped version for candidate. For dht local candidates {@code otherVer} is near local candidate version.
     * For near local candidates {@code otherVer} is dht mapped candidate version.
     *
     * @param otherVer Alternative candidate version.
     * @return {@code True} if other version was set, {@code false} if other version is already set.
     */
    public boolean otherVersion(GridCacheVersion otherVer) {
        assert otherVer != null;

        if (this.otherVer == null) {
            this.otherVer = otherVer;

            return true;
        }

        return this.otherVer.equals(otherVer);
    }

    /**
     * Sets owned version for proper lock ordering when remote candidate is added.
     *
     * @param ownerVer Version of owned candidate by the moment this candidate was added.
     * @return {@code True} if owned version was set, {@code false} otherwise.
     */
    public boolean ownerVersion(GridCacheVersion ownerVer) {
        assert ownerVer != null;

        if (this.ownerVer == null) {
            this.ownerVer = ownerVer;

            return true;
        }

        return this.ownerVer.equals(ownerVer);
    }

    /**
     * @return Version of owned candidate by the time this candidate was added, or {@code null}
     *      if there were no owned candidates.
     */
    @Nullable public GridCacheVersion ownerVersion() {
        return ownerVer;
    }

    /**
     * @return Thread ID.
     * @see Thread#getId()
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return {@code True} if lock is local.
     */
    public boolean local() {
        return LOCAL.get(flags());
    }

    /**
     * @return {@code True} if transaction flag is set.
     */
    public boolean tx() {
        return TX.get(flags());
    }

    /**
     * @return {@code True} if implicit transaction.
     */
    public boolean singleImplicit() {
        return SINGLE_IMPLICIT.get(flags());
    }

    /**
     * @return Near local flag.
     */
    public boolean nearLocal() {
        return NEAR_LOCAL.get(flags());
    }

    /**
     * @return Near local flag.
     */
    public boolean dhtLocal() {
        return DHT_LOCAL.get(flags());
    }

    /**
     * @return Serializable transaction flag.
     */
    public boolean serializable() {
        return serOrder != null;
    }

    /**
     * @return Version for serializable transactions ordering.
     */
    @Nullable public GridCacheVersion serializableOrder() {
        return serOrder;
    }

    /**
     * @return Read lock flag.
     */
    public boolean read() {
        return READ.get(flags());
    }

    /**
     * @return {@code True} if this candidate is a reentry.
     */
    public boolean reentry() {
        return REENTRY.get(flags());
    }

    /**
     * Sets reentry flag.
     */
    public void setReentry() {
        mask(REENTRY, true);
    }

    /**
     * @return Ready flag.
     */
    public boolean ready() {
        return READY.get(flags());
    }

    /**
     * Sets ready flag.
     */
    public void setReady() {
        mask(READY, true);
    }

    /**
     * @return {@code True} if lock was released.
     */
    public boolean used() {
        return USED.get(flags());
    }

    /**
     * Sets used flag.
     */
    public void setUsed() {
        mask(USED, true);
    }

    /**
     * @return Removed flag.
     */
    public boolean removed() {
        return REMOVED.get(flags());
    }

    /**
     * Sets removed flag.
     */
    public void setRemoved() {
        mask(REMOVED, true);
    }

    /**
     * @return {@code True} if is or was an owner.
     */
    public boolean owner() {
        return OWNER.get(flags());
    }

    /**
     * Sets owner flag.
     */
    public void setOwner() {
        mask(OWNER, true);
    }

    /**
     * @return Lock that comes before in the same thread, possibly <tt>null</tt>.
     */
    @Nullable public GridCacheMvccCandidate previous() {
        return prev;
    }

    /**
     * @param prev Lock that comes before in the same thread.
     */
    public void previous(GridCacheMvccCandidate prev) {
        assert threadId == prev.threadId : "Invalid threadId [this=" + this + ", prev=" + prev + ']';

        this.prev = prev;
    }

    /**
     *
     * @return Gets next candidate in this thread.
     */
    public GridCacheMvccCandidate next() {
        return next;
    }

    /**
     * @param next Next candidate in this thread.
     */
    public void next(GridCacheMvccCandidate next) {
        this.next = next;
    }

    /**
     * @return Key.
     */
    public IgniteTxKey key() {
        GridCacheEntryEx parent0 = parent;

        if (parent0 == null)
            throw new IllegalStateException("Parent entry was not initialized for MVCC candidate: " + this);

        return parent0.txKey();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(int idx) {
        assert idx == 0 : idx;

        return this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean hasCandidate(GridCacheVersion ver) {
        return this.ver.equals(ver);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        IgniteUtils.writeUuid(out, nodeId);

        out.writeBoolean(ver == null);

        if (ver != null) {
            out.writeBoolean(ver instanceof GridCacheVersionEx);

            ver.writeExternal(out);
        }

        out.writeLong(threadId);
        out.writeLong(id);
        out.writeShort(flags());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = IgniteUtils.readUuid(in);

        if (!in.readBoolean()) {
            ver = in.readBoolean() ? new GridCacheVersionEx() : new GridCacheVersion();

            ver.readExternal(in);
        }

        threadId = in.readLong();
        id = in.readLong();

        short flags = in.readShort();

        mask(OWNER, OWNER.get(flags));
        mask(USED, USED.get(flags));
        mask(TX, TX.get(flags));
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridCacheMvccCandidate o) {
        if (o == this)
            return 0;

        int c = ver.compareTo(o.ver);

        // This is done, so compare and equals methods will be consistent.
        if (c == 0)
            return key().equals(o.key()) ? 0 : id < o.id ? -1 : 1;

        return c;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean equals(Object o) {
        if (o == null)
            return false;

        if (o == this)
            return true;

        GridCacheMvccCandidate other = (GridCacheMvccCandidate)o;

        assert key() != null && other.key() != null : "Key is null [this=" + this + ", other=" + o + ']';

        return ver.equals(other.ver) && key().equals(other.key());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return ver.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        GridCacheMvccCandidate prev = previous();
        GridCacheMvccCandidate next = next();

        return S.toString(GridCacheMvccCandidate.class, this,
            "key", parent == null ? null : parent.key(), true,
            "masks", Mask.toString(flags()), false,
            "prevVer", prev == null ? null : prev.version(), false,
            "nextVer", next == null ? null : next.version(), false);
    }

    /**
     * Mask.
     */
    @SuppressWarnings({"PackageVisibleInnerClass"})
    enum Mask {
        /** */
        LOCAL(0x01),

        /** */
        OWNER(0x02),

        /** */
        READY(0x04),

        /** */
        REENTRY(0x08),

        /** */
        USED(0x10),

        /** */
        TX(0x40),

        /** */
        SINGLE_IMPLICIT(0x80),

        /** */
        DHT_LOCAL(0x100),

        /** */
        NEAR_LOCAL(0x200),

        /** */
        REMOVED(0x400),

        /** */
        READ(0x800);

        /** All mask values. */
        private static final Mask[] MASKS = values();

        /** Mask bit. */
        private final short bit;

        /**
         * @param bit Mask value.
         */
        Mask(int bit) {
            this.bit = (short)bit;
        }

        /**
         * @param flags Flags to check.
         * @return {@code True} if mask is set.
         */
        boolean get(short flags) {
            return (flags & bit) == bit;
        }

        /**
         * @param flags Flags.
         * @param on Mask to set.
         * @return Updated flags.
         */
        short set(short flags, boolean on) {
            return (short)(on ? flags | bit : flags & ~bit);
        }

        /**
         * @param flags Flags to check.
         * @return {@code 1} if mask is set, {@code 0} otherwise.
         */
        int bit(short flags) {
            return get(flags) ? 1 : 0;
        }

        /**
         * @param flags Flags.
         * @return String builder containing all flags.
         */
        static String toString(short flags) {
            SB sb = new SB();

            for (Mask m : MASKS) {
                if (m.ordinal() != 0)
                    sb.a('|');

                sb.a(m.name().toLowerCase()).a('=').a(m.bit(flags));
            }

            return sb.toString();
        }
    }
}