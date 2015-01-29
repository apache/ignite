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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Transaction prepare request for optimistic and eventually consistent
 * transactions.
 */
public class GridDistributedTxPrepareRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Thread ID. */
    @GridToStringInclude
    private long threadId;

    /** Transaction concurrency. */
    @GridToStringInclude
    private IgniteTxConcurrency concurrency;

    /** Transaction isolation. */
    @GridToStringInclude
    private IgniteTxIsolation isolation;

    /** Commit version for EC transactions. */
    @GridToStringInclude
    private GridCacheVersion writeVer;

    /** Transaction timeout. */
    @GridToStringInclude
    private long timeout;

    /** Invalidation flag. */
    @GridToStringInclude
    private boolean invalidate;

    /** Transaction read set. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> reads;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> readsBytes;

    /** Transaction write entries. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<IgniteTxEntry<K, V>> writes;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> writesBytes;

    /** DHT versions to verify. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey<K>, GridCacheVersion> dhtVers;

    /** Serialized map. */
    @GridToStringExclude
    private byte[] dhtVersBytes;

    /** Group lock key, if any. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteTxKey grpLockKey;

    /** Group lock key bytes. */
    @GridToStringExclude
    private byte[] grpLockKeyBytes;

    /** Partition lock flag. */
    private boolean partLock;

    /** Expected transaction size. */
    private int txSize;

    /** Transaction nodes mapping (primary node -> related backup nodes). */
    @GridDirectTransient
    private Map<UUID, Collection<UUID>> txNodes;

    /** */
    private byte[] txNodesBytes;

    /** One phase commit flag. */
    private boolean onePhaseCommit;

    /** System flag. */
    private boolean sys;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDistributedTxPrepareRequest() {
        /* No-op. */
    }

    /**
     * @param tx Cache transaction.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param grpLockKey Group lock key.
     * @param partLock {@code True} if preparing group-lock transaction with partition lock.
     * @param txNodes Transaction nodes mapping.
     * @param onePhaseCommit One phase commit flag.
     */
    public GridDistributedTxPrepareRequest(
        IgniteTxEx<K, V> tx,
        @Nullable Collection<IgniteTxEntry<K, V>> reads,
        Collection<IgniteTxEntry<K, V>> writes,
        IgniteTxKey grpLockKey,
        boolean partLock,
        Map<UUID, Collection<UUID>> txNodes,
        boolean onePhaseCommit
    ) {
        super(tx.xidVersion(), 0);

        writeVer = tx.writeVersion();
        threadId = tx.threadId();
        concurrency = tx.concurrency();
        isolation = tx.isolation();
        timeout = tx.timeout();
        invalidate = tx.isInvalidate();
        txSize = tx.size();
        sys = tx.system();

        this.reads = reads;
        this.writes = writes;
        this.grpLockKey = grpLockKey;
        this.partLock = partLock;
        this.txNodes = txNodes;
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return Transaction nodes mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * @return System flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * Adds version to be verified on remote node.
     *
     * @param key Key for which version is verified.
     * @param dhtVer DHT version to check.
     */
    public void addDhtVersion(IgniteTxKey<K> key, @Nullable GridCacheVersion dhtVer) {
        if (dhtVers == null)
            dhtVers = new HashMap<>();

        dhtVers.put(key, dhtVer);
    }

    /**
     * @return Map of versions to be verified.
     */
    public Map<IgniteTxKey<K>, GridCacheVersion> dhtVersions() {
        return dhtVers == null ? Collections.<IgniteTxKey<K>, GridCacheVersion>emptyMap() : dhtVers;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion writeVersion() { return writeVer; }

    /**
     * @return Invalidate flag.
     */
    public boolean isInvalidate() { return invalidate; }

    /**
     * @return Transaction timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Concurrency.
     */
    public IgniteTxConcurrency concurrency() {
        return concurrency;
    }

    /**
     * @return Isolation level.
     */
    public IgniteTxIsolation isolation() {
        return isolation;
    }

    /**
     * @return Read set.
     */
    public Collection<IgniteTxEntry<K, V>> reads() {
        return reads;
    }

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry<K, V>> writes() {
        return writes;
    }

    /**
     * @param reads Reads.
     */
    protected void reads(Collection<IgniteTxEntry<K, V>> reads) {
        this.reads = reads;
    }

    /**
     * @param writes Writes.
     */
    protected void writes(Collection<IgniteTxEntry<K, V>> writes) {
        this.writes = writes;
    }

    /**
     * @return Group lock key if preparing group-lock transaction.
     */
    @Nullable public IgniteTxKey groupLockKey() {
        return grpLockKey;
    }

    /**
     * @return {@code True} if preparing group-lock transaction with partition lock.
     */
    public boolean partitionLock() {
        return partLock;
    }

    /**
     * @return Expected transaction size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (writes != null) {
            marshalTx(writes, ctx);

            writesBytes = new ArrayList<>(writes.size());

            for (IgniteTxEntry<K, V> e : writes)
                writesBytes.add(ctx.marshaller().marshal(e));
        }

        if (reads != null) {
            marshalTx(reads, ctx);

            readsBytes = new ArrayList<>(reads.size());

            for (IgniteTxEntry<K, V> e : reads)
                readsBytes.add(ctx.marshaller().marshal(e));
        }

        if (grpLockKey != null && grpLockKeyBytes == null)
            grpLockKeyBytes = ctx.marshaller().marshal(grpLockKey);

        if (dhtVers != null && dhtVersBytes == null)
            dhtVersBytes = ctx.marshaller().marshal(dhtVers);

        if (txNodes != null)
            txNodesBytes = ctx.marshaller().marshal(txNodes);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (writesBytes != null) {
            writes = new ArrayList<>(writesBytes.size());

            for (byte[] arr : writesBytes)
                writes.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(writes, false, ctx, ldr);
        }

        if (readsBytes != null) {
            reads = new ArrayList<>(readsBytes.size());

            for (byte[] arr : readsBytes)
                reads.add(ctx.marshaller().<IgniteTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(reads, false, ctx, ldr);
        }

        if (grpLockKeyBytes != null && grpLockKey == null)
            grpLockKey = ctx.marshaller().unmarshal(grpLockKeyBytes, ldr);

        if (dhtVersBytes != null && dhtVers == null)
            dhtVers = ctx.marshaller().unmarshal(dhtVersBytes, ldr);

        if (txNodesBytes != null)
            txNodes = ctx.marshaller().unmarshal(txNodesBytes, ldr);
    }

    /**
     *
     * @param out Output.
     * @param col Set to write.
     * @throws IOException If write failed.
     */
    private void writeCollection(ObjectOutput out, Collection<IgniteTxEntry<K, V>> col) throws IOException {
        boolean empty = F.isEmpty(col);

        if (!empty) {
            out.writeInt(col.size());

            for (IgniteTxEntry<K, V> e : col) {
                V val = e.value();
                boolean hasWriteVal = e.hasWriteValue();
                boolean hasReadVal = e.hasReadValue();

                try {
                    // Don't serialize value if invalidate is set to true.
                    if (invalidate)
                        e.value(null, false, false);

                    out.writeObject(e);
                }
                finally {
                    // Set original value back.
                    e.value(val, hasWriteVal, hasReadVal);
                }
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private Collection<IgniteTxEntry<K, V>> readCollection(ObjectInput in) throws IOException,
        ClassNotFoundException {
        List<IgniteTxEntry<K, V>> col = null;

        int size = in.readInt();

        // Check null flag.
        if (size != -1) {
            col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add((IgniteTxEntry<K, V>)in.readObject());
        }

        return col == null ? Collections.<IgniteTxEntry<K,V>>emptyList() : col;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDistributedTxPrepareRequest _clone = new GridDistributedTxPrepareRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedTxPrepareRequest _clone = (GridDistributedTxPrepareRequest)_msg;

        _clone.threadId = threadId;
        _clone.concurrency = concurrency;
        _clone.isolation = isolation;
        _clone.writeVer = writeVer;
        _clone.timeout = timeout;
        _clone.invalidate = invalidate;
        _clone.reads = reads;
        _clone.readsBytes = readsBytes;
        _clone.writes = writes;
        _clone.writesBytes = writesBytes;
        _clone.dhtVers = dhtVers;
        _clone.dhtVersBytes = dhtVersBytes;
        _clone.grpLockKey = grpLockKey;
        _clone.grpLockKeyBytes = grpLockKeyBytes;
        _clone.partLock = partLock;
        _clone.txSize = txSize;
        _clone.txNodes = txNodes;
        _clone.txNodesBytes = txNodesBytes;
        _clone.onePhaseCommit = onePhaseCommit;
        _clone.sys = sys;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 8:
                if (!commState.putEnum(concurrency))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putByteArray(dhtVersBytes))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putByteArray(grpLockKeyBytes))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putBoolean(invalidate))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putEnum(isolation))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putBoolean(onePhaseCommit))
                    return false;

                commState.idx++;

            case 14:
                if (!commState.putBoolean(partLock))
                    return false;

                commState.idx++;

            case 15:
                if (readsBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(readsBytes.size()))
                            return false;

                        commState.it = readsBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 16:
                if (!commState.putBoolean(sys))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putLong(threadId))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 19:
                if (!commState.putByteArray(txNodesBytes))
                    return false;

                commState.idx++;

            case 20:
                if (!commState.putInt(txSize))
                    return false;

                commState.idx++;

            case 21:
                if (!commState.putCacheVersion(writeVer))
                    return false;

                commState.idx++;

            case 22:
                if (writesBytes != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(writesBytes.size()))
                            return false;

                        commState.it = writesBytes.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByteArray((byte[])commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 8:
                if (buf.remaining() < 1)
                    return false;

                byte concurrency0 = commState.getByte();

                concurrency = IgniteTxConcurrency.fromOrdinal(concurrency0);

                commState.idx++;

            case 9:
                byte[] dhtVersBytes0 = commState.getByteArray();

                if (dhtVersBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                dhtVersBytes = dhtVersBytes0;

                commState.idx++;

            case 10:
                byte[] grpLockKeyBytes0 = commState.getByteArray();

                if (grpLockKeyBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                grpLockKeyBytes = grpLockKeyBytes0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 1)
                    return false;

                invalidate = commState.getBoolean();

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                byte isolation0 = commState.getByte();

                isolation = IgniteTxIsolation.fromOrdinal(isolation0);

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                onePhaseCommit = commState.getBoolean();

                commState.idx++;

            case 14:
                if (buf.remaining() < 1)
                    return false;

                partLock = commState.getBoolean();

                commState.idx++;

            case 15:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (readsBytes == null)
                        readsBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        readsBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 16:
                if (buf.remaining() < 1)
                    return false;

                sys = commState.getBoolean();

                commState.idx++;

            case 17:
                if (buf.remaining() < 8)
                    return false;

                threadId = commState.getLong();

                commState.idx++;

            case 18:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 19:
                byte[] txNodesBytes0 = commState.getByteArray();

                if (txNodesBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                txNodesBytes = txNodesBytes0;

                commState.idx++;

            case 20:
                if (buf.remaining() < 4)
                    return false;

                txSize = commState.getInt();

                commState.idx++;

            case 21:
                GridCacheVersion writeVer0 = commState.getCacheVersion();

                if (writeVer0 == CACHE_VER_NOT_READ)
                    return false;

                writeVer = writeVer0;

                commState.idx++;

            case 22:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (writesBytes == null)
                        writesBytes = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        writesBytes.add((byte[])_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareRequest.class, this,
            "super", super.toString());
    }
}
