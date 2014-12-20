/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
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
    private GridCacheTxConcurrency concurrency;

    /** Transaction isolation. */
    @GridToStringInclude
    private GridCacheTxIsolation isolation;

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
    private Collection<GridCacheTxEntry<K, V>> reads;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> readsBytes;

    /** Transaction write entries. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheTxEntry<K, V>> writes;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> writesBytes;

    /** DHT versions to verify. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<GridCacheTxKey<K>, GridCacheVersion> dhtVers;

    /** Serialized map. */
    @GridToStringExclude
    private byte[] dhtVersBytes;

    /** Group lock key, if any. */
    @GridToStringInclude
    @GridDirectTransient
    private GridCacheTxKey grpLockKey;

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
        GridCacheTxEx<K, V> tx,
        @Nullable Collection<GridCacheTxEntry<K, V>> reads,
        Collection<GridCacheTxEntry<K, V>> writes,
        GridCacheTxKey grpLockKey,
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
    public void addDhtVersion(GridCacheTxKey<K> key, @Nullable GridCacheVersion dhtVer) {
        if (dhtVers == null)
            dhtVers = new HashMap<>();

        dhtVers.put(key, dhtVer);
    }

    /**
     * @return Map of versions to be verified.
     */
    public Map<GridCacheTxKey<K>, GridCacheVersion> dhtVersions() {
        return dhtVers == null ? Collections.<GridCacheTxKey<K>, GridCacheVersion>emptyMap() : dhtVers;
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
    public GridCacheTxConcurrency concurrency() {
        return concurrency;
    }

    /**
     * @return Isolation level.
     */
    public GridCacheTxIsolation isolation() {
        return isolation;
    }

    /**
     * @return Read set.
     */
    public Collection<GridCacheTxEntry<K, V>> reads() {
        return reads;
    }

    /**
     * @return Write entries.
     */
    public Collection<GridCacheTxEntry<K, V>> writes() {
        return writes;
    }

    /**
     * @param reads Reads.
     */
    protected void reads(Collection<GridCacheTxEntry<K, V>> reads) {
        this.reads = reads;
    }

    /**
     * @param writes Writes.
     */
    protected void writes(Collection<GridCacheTxEntry<K, V>> writes) {
        this.writes = writes;
    }

    /**
     * @return Group lock key if preparing group-lock transaction.
     */
    @Nullable public GridCacheTxKey groupLockKey() {
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

            for (GridCacheTxEntry<K, V> e : writes)
                writesBytes.add(ctx.marshaller().marshal(e));
        }

        if (reads != null) {
            marshalTx(reads, ctx);

            readsBytes = new ArrayList<>(reads.size());

            for (GridCacheTxEntry<K, V> e : reads)
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
                writes.add(ctx.marshaller().<GridCacheTxEntry<K, V>>unmarshal(arr, ldr));

            unmarshalTx(writes, false, ctx, ldr);
        }

        if (readsBytes != null) {
            reads = new ArrayList<>(readsBytes.size());

            for (byte[] arr : readsBytes)
                reads.add(ctx.marshaller().<GridCacheTxEntry<K, V>>unmarshal(arr, ldr));

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
    private void writeCollection(ObjectOutput out, Collection<GridCacheTxEntry<K, V>> col) throws IOException {
        boolean empty = F.isEmpty(col);

        if (!empty) {
            out.writeInt(col.size());

            for (GridCacheTxEntry<K, V> e : col) {
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
    @Nullable private Collection<GridCacheTxEntry<K, V>> readCollection(ObjectInput in) throws IOException,
        ClassNotFoundException {
        List<GridCacheTxEntry<K, V>> col = null;

        int size = in.readInt();

        // Check null flag.
        if (size != -1) {
            col = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                col.add((GridCacheTxEntry<K, V>)in.readObject());
        }

        return col == null ? Collections.<GridCacheTxEntry<K,V>>emptyList() : col;
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
        _clone.commitVer = commitVer;
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
                if (!commState.putCacheVersion(commitVer))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putEnum(concurrency))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putByteArray(dhtVersBytes))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putByteArray(grpLockKeyBytes))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean(invalidate))
                    return false;

                commState.idx++;

            case 13:
                if (!commState.putEnum(isolation))
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
                if (!commState.putLong(threadId))
                    return false;

                commState.idx++;

            case 17:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 18:
                if (!commState.putByteArray(txNodesBytes))
                    return false;

                commState.idx++;

            case 19:
                if (!commState.putInt(txSize))
                    return false;

                commState.idx++;

            case 20:
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

            case 21:
                if (!commState.putBoolean(sys))
                    return false;

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
                GridCacheVersion commitVer0 = commState.getCacheVersion();

                if (commitVer0 == CACHE_VER_NOT_READ)
                    return false;

                commitVer = commitVer0;

                commState.idx++;

            case 9:
                if (buf.remaining() < 1)
                    return false;

                byte concurrency0 = commState.getByte();

                concurrency = GridCacheTxConcurrency.fromOrdinal(concurrency0);

                commState.idx++;

            case 10:
                byte[] dhtVersBytes0 = commState.getByteArray();

                if (dhtVersBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                dhtVersBytes = dhtVersBytes0;

                commState.idx++;

            case 11:
                byte[] grpLockKeyBytes0 = commState.getByteArray();

                if (grpLockKeyBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                grpLockKeyBytes = grpLockKeyBytes0;

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                invalidate = commState.getBoolean();

                commState.idx++;

            case 13:
                if (buf.remaining() < 1)
                    return false;

                byte isolation0 = commState.getByte();

                isolation = GridCacheTxIsolation.fromOrdinal(isolation0);

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
                if (buf.remaining() < 8)
                    return false;

                threadId = commState.getLong();

                commState.idx++;

            case 17:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 18:
                byte[] txNodesBytes0 = commState.getByteArray();

                if (txNodesBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                txNodesBytes = txNodesBytes0;

                commState.idx++;

            case 19:
                if (buf.remaining() < 4)
                    return false;

                txSize = commState.getInt();

                commState.idx++;

            case 20:
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

            case 21:
                if (buf.remaining() < 1)
                    return false;

                sys = commState.getBoolean();

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
