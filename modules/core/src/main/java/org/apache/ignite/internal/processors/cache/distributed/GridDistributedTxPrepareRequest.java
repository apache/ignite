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
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.transactions.*;
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
    private GridCacheVersion commitVer;

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
     */
    public GridDistributedTxPrepareRequest(
        IgniteInternalTx<K, V> tx,
        @Nullable Collection<IgniteTxEntry<K, V>> reads,
        Collection<IgniteTxEntry<K, V>> writes,
        IgniteTxKey grpLockKey,
        boolean partLock,
        Map<UUID, Collection<UUID>> txNodes
    ) {
        super(tx.xidVersion(), 0);

        commitVer = null;
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
    public GridCacheVersion commitVersion() { return commitVer; }

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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeField("commitVer", commitVer, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("concurrency", concurrency != null ? (byte)concurrency.ordinal() : -1, MessageFieldType.BYTE))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeField("dhtVersBytes", dhtVersBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeField("grpLockKeyBytes", grpLockKeyBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeField("invalidate", invalidate, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeField("isolation", isolation != null ? (byte)isolation.ordinal() : -1, MessageFieldType.BYTE))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeField("partLock", partLock, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollectionField("readsBytes", readsBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeField("sys", sys, MessageFieldType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeField("threadId", threadId, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeField("timeout", timeout, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeField("txNodesBytes", txNodesBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeField("txSize", txSize, MessageFieldType.INT))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollectionField("writesBytes", writesBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 8:
                commitVer = reader.readField("commitVer", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                byte concurrencyOrd;

                concurrencyOrd = reader.readField("concurrency", MessageFieldType.BYTE);

                if (!reader.isLastRead())
                    return false;

                concurrency = IgniteTxConcurrency.fromOrdinal(concurrencyOrd);

                readState++;

            case 10:
                dhtVersBytes = reader.readField("dhtVersBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                grpLockKeyBytes = reader.readField("grpLockKeyBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                invalidate = reader.readField("invalidate", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                byte isolationOrd;

                isolationOrd = reader.readField("isolation", MessageFieldType.BYTE);

                if (!reader.isLastRead())
                    return false;

                isolation = IgniteTxIsolation.fromOrdinal(isolationOrd);

                readState++;

            case 14:
                partLock = reader.readField("partLock", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                readsBytes = reader.readCollectionField("readsBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 16:
                sys = reader.readField("sys", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 17:
                threadId = reader.readField("threadId", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 18:
                timeout = reader.readField("timeout", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 19:
                txNodesBytes = reader.readField("txNodesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 20:
                txSize = reader.readField("txSize", MessageFieldType.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 21:
                writesBytes = reader.readCollectionField("writesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareRequest.class, this,
            "super", super.toString());
    }
}
