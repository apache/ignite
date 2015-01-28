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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;

/**
 * Transaction entry. Note that it is essential that this class does not override
 * {@link #equals(Object)} method, as transaction entries should use referential
 * equality.
 */
public class IgniteTxEntry<K, V> implements GridPeerDeployAware, Externalizable, IgniteOptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

    /** Owning transaction. */
    @GridToStringExclude
    private IgniteTxEx<K, V> tx;

    /** Cache key. */
    @GridToStringInclude
    private K key;

    /** Key bytes. */
    private byte[] keyBytes;

    /** Cache ID. */
    private int cacheId;

    /** Transient tx key. */
    private IgniteTxKey<K> txKey;

    /** Cache value. */
    @GridToStringInclude
    private TxEntryValueHolder<K, V> val = new TxEntryValueHolder<>();

    /** Visible value for peek. */
    @GridToStringInclude
    private TxEntryValueHolder<K, V> prevVal = new TxEntryValueHolder<>();

    /** Filter bytes. */
    private byte[] filterBytes;

    /** Transform. */
    @GridToStringInclude
    private Collection<T2<EntryProcessor<K, V, ?>, Object[]>> entryProcessorsCol;

    /** Transform closure bytes. */
    @GridToStringExclude
    private byte[] transformClosBytes;

    /** Time to live. */
    private long ttl;

    /** DR expire time (explicit) */
    private long drExpireTime = -1L;

    /** Explicit lock version if there is one. */
    @GridToStringInclude
    private GridCacheVersion explicitVer;

    /** DHT version. */
    private transient volatile GridCacheVersion dhtVer;

    /** Put filters. */
    @GridToStringInclude
    private IgnitePredicate<CacheEntry<K, V>>[] filters;

    /** Flag indicating whether filters passed. Used for fast-commit transactions. */
    private boolean filtersPassed;

    /** Flag indicating that filter is set and can not be replaced. */
    private transient boolean filtersSet;

    /** Underlying cache entry. */
    private transient volatile GridCacheEntryEx<K, V> entry;

    /** Cache registry. */
    private transient GridCacheContext<K, V> ctx;

    /** Prepared flag to prevent multiple candidate add. */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private transient AtomicBoolean prepared = new AtomicBoolean();

    /** Lock flag for colocated cache. */
    private transient boolean locked;

    /** Assigned node ID (required only for partitioned cache). */
    private transient UUID nodeId;

    /** Flag if this node is a back up node. */
    private boolean locMapped;

    /** Group lock entry flag. */
    private boolean grpLock;

    /** Flag indicating if this entry should be transferred to remote node. */
    private boolean transferRequired;

    /** Deployment enabled flag. */
    private boolean depEnabled;

    /** Data center replication version. */
    private GridCacheVersion drVer;

    /** Expiry policy. */
    private ExpiryPolicy expiryPlc;

    /** Expiry policy transfer flag. */
    private boolean transferExpiryPlc;

    /**
     * Required by {@link Externalizable}
     */
    public IgniteTxEntry() {
        /* No-op. */
    }

    /**
     * This constructor is meant for remote transactions.
     *
     * @param ctx Cache registry.
     * @param tx Owning transaction.
     * @param op Operation.
     * @param val Value.
     * @param ttl Time to live.
     * @param drExpireTime DR expire time.
     * @param entry Cache entry.
     * @param drVer Data center replication version.
     */
    public IgniteTxEntry(GridCacheContext<K, V> ctx,
        IgniteTxEx<K, V> tx,
        GridCacheOperation op,
        V val,
        long ttl,
        long drExpireTime,
        GridCacheEntryEx<K, V> entry,
        @Nullable GridCacheVersion drVer) {
        assert ctx != null;
        assert tx != null;
        assert op != null;
        assert entry != null;

        this.ctx = ctx;
        this.tx = tx;
        this.val.value(op, val, false, false);
        this.entry = entry;
        this.ttl = ttl;
        this.drExpireTime = drExpireTime;
        this.drVer = drVer;

        key = entry.key();
        keyBytes = entry.keyBytes();

        cacheId = entry.context().cacheId();

        depEnabled = ctx.gridDeploy().enabled();
    }

    /**
     * This constructor is meant for local transactions.
     *
     * @param ctx Cache registry.
     * @param tx Owning transaction.
     * @param op Operation.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param ttl Time to live.
     * @param entry Cache entry.
     * @param filters Put filters.
     * @param drVer Data center replication version.
     */
    public IgniteTxEntry(GridCacheContext<K, V> ctx,
        IgniteTxEx<K, V> tx,
        GridCacheOperation op,
        V val,
        EntryProcessor<K, V, ?> entryProcessor,
        Object[] invokeArgs,
        long ttl,
        GridCacheEntryEx<K, V> entry,
        IgnitePredicate<CacheEntry<K, V>>[] filters,
        GridCacheVersion drVer) {
        assert ctx != null;
        assert tx != null;
        assert op != null;
        assert entry != null;

        this.ctx = ctx;
        this.tx = tx;
        this.val.value(op, val, false, false);
        this.entry = entry;
        this.ttl = ttl;
        this.filters = filters;
        this.drVer = drVer;

        if (entryProcessor != null)
            addEntryProcessor(entryProcessor, invokeArgs);

        key = entry.key();
        keyBytes = entry.keyBytes();

        cacheId = entry.context().cacheId();

        depEnabled = ctx.gridDeploy().enabled();
    }

    /**
     * @return Cache context for this tx entry.
     */
    public GridCacheContext<K, V> context() {
        return ctx;
    }

    /**
     * @return Flag indicating if this entry is affinity mapped to the same node.
     */
    public boolean locallyMapped() {
        return locMapped;
    }

    /**
     * @param locMapped Flag indicating if this entry is affinity mapped to the same node.
     */
    public void locallyMapped(boolean locMapped) {
        this.locMapped = locMapped;
    }

    /**
     * @return {@code True} if this entry was added in group lock transaction and
     *      this is not a group lock entry.
     */
    public boolean groupLockEntry() {
        return grpLock;
    }

    /**
     * @param grpLock {@code True} if this entry was added in group lock transaction and
     *      this is not a group lock entry.
     */
    public void groupLockEntry(boolean grpLock) {
        this.grpLock = grpLock;
    }

    /**
     * @param transferRequired Sets flag indicating that transfer is required to remote node.
     */
    public void transferRequired(boolean transferRequired) {
        this.transferRequired = transferRequired;
    }

    /**
     * @return Flag indicating whether transfer is required to remote nodes.
     */
    public boolean transferRequired() {
        return transferRequired;
    }

    /**
     * @param ctx Context.
     * @return Clean copy of this entry.
     */
    public IgniteTxEntry<K, V> cleanCopy(GridCacheContext<K, V> ctx) {
        IgniteTxEntry<K, V> cp = new IgniteTxEntry<>();

        cp.key = key;
        cp.cacheId = cacheId;
        cp.ctx = ctx;

        cp.val = new TxEntryValueHolder<>();

        cp.keyBytes = keyBytes;
        cp.filters = filters;
        cp.val.value(val.op(), val.value(), val.hasWriteValue(), val.hasReadValue());
        cp.val.valueBytes(val.valueBytes());
        cp.entryProcessorsCol = entryProcessorsCol;
        cp.ttl = ttl;
        cp.drExpireTime = drExpireTime;
        cp.explicitVer = explicitVer;
        cp.grpLock = grpLock;
        cp.depEnabled = depEnabled;
        cp.drVer = drVer;
        cp.expiryPlc = expiryPlc;

        return cp;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer DHT version.
     */
    public void dhtVersion(GridCacheVersion dhtVer) {
        this.dhtVer = dhtVer;
    }

    /**
     * @return {@code True} if tx entry was marked as locked.
     */
    public boolean locked() {
        return locked;
    }

    /**
     * Marks tx entry as locked.
     */
    public void markLocked() {
        locked = true;
    }

    /**
     * @param val Value to set.
     */
    void setAndMarkValid(V val) {
        setAndMarkValid(op(), val, this.val.hasWriteValue(), this.val.hasReadValue());
    }

    /**
     * @param op Operation.
     * @param val Value to set.
     */
    void setAndMarkValid(GridCacheOperation op, V val) {
        setAndMarkValid(op, val, this.val.hasWriteValue(), this.val.hasReadValue());
    }

    /**
     * @param op Operation.
     * @param val Value to set.
     * @param hasReadVal Has read value flag.
     * @param hasWriteVal Has write value flag.
     */
    void setAndMarkValid(GridCacheOperation op, V val, boolean hasWriteVal, boolean hasReadVal) {
        this.val.value(op, val, hasWriteVal, hasReadVal);

        markValid();
    }

    /**
     * Marks this entry as value-has-bean-read. Effectively, makes values enlisted to transaction visible
     * to further peek operations.
     */
    void markValid() {
        prevVal.value(val.op(), val.value(), val.hasWriteValue(), val.hasReadValue());
    }

    /**
     * Marks entry as prepared.
     *
     * @return True if entry was marked prepared by this call.
     */
    boolean markPrepared() {
        return prepared.compareAndSet(false, true);
    }

    /**
     * @return Entry key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Tx key.
     */
    public IgniteTxKey<K> txKey() {
        if (txKey == null)
            txKey = new IgniteTxKey<>(key, cacheId);

        return txKey;
    }

    /**
     *
     * @return Key bytes.
     */
    @Nullable public byte[] keyBytes() {
        byte[] bytes = keyBytes;

        if (bytes == null && entry != null) {
            bytes = entry.keyBytes();

            keyBytes = bytes;
        }

        return bytes;
    }

    /**
     * @param keyBytes Key bytes.
     */
    public void keyBytes(byte[] keyBytes) {
        initKeyBytes(keyBytes);
    }

    /**
     * @return Underlying cache entry.
     */
    public GridCacheEntryEx<K, V> cached() {
        return entry;
    }

    /**
     * @param entry Cache entry.
     * @param keyBytes Key bytes, possibly {@code null}.
     */
    public void cached(GridCacheEntryEx<K,V> entry, @Nullable byte[] keyBytes) {
        assert entry != null;

        assert entry.context() == ctx : "Invalid entry assigned to tx entry [txEntry=" + this +
            ", entry=" + entry + ", ctxNear=" + ctx.isNear() + ", ctxDht=" + ctx.isDht() + ']';

        this.entry = entry;

        initKeyBytes(keyBytes);
    }

    /**
     * Initialized key bytes locally and on the underlying entry.
     *
     * @param bytes Key bytes to initialize.
     */
    private void initKeyBytes(@Nullable byte[] bytes) {
        if (bytes != null) {
            keyBytes = bytes;

            while (true) {
                try {
                    if (entry != null)
                        entry.keyBytes(bytes);

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    entry = ctx.cache().entryEx(key);
                }
            }
        }
        else if (entry != null) {
            bytes = entry.keyBytes();

            if (bytes != null)
                keyBytes = bytes;
        }
    }

    /**
     * @return Entry value.
     */
    @Nullable public V value() {
        return val.value();
    }

    /**
     * @return {@code True} if has value explicitly set.
     */
    public boolean hasValue() {
        return val.hasValue();
    }

    /**
     * @return {@code True} if has write value set.
     */
    public boolean hasWriteValue() {
        return val.hasWriteValue();
    }

    /**
     * @return {@code True} if has read value set.
     */
    public boolean hasReadValue() {
        return val.hasReadValue();
    }

    /**
     * @return Value visible for peek.
     */
    @Nullable public V previousValue() {
        return prevVal.value();
    }

    /**
     * @return {@code True} if has previous value explicitly set.
     */
    boolean hasPreviousValue() {
        return prevVal.hasValue();
    }

    /**
     * @return Previous operation to revert entry in case of filter failure.
     */
    @Nullable public GridCacheOperation previousOperation() {
        return prevVal.op();
    }

    /**
     * @return Value bytes.
     */
    @Nullable public byte[] valueBytes() {
        return val.valueBytes();
    }

    /**
     * @param valBytes Value bytes.
     */
    public void valueBytes(@Nullable byte[] valBytes) {
        val.valueBytes(valBytes);
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param ttl Time to live.
     */
    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return DR expire time.
     */
    public long drExpireTime() {
        return drExpireTime;
    }

    /**
     * @param drExpireTime DR expire time.
     */
    public void drExpireTime(long drExpireTime) {
        this.drExpireTime = drExpireTime;
    }

    /**
     * @param val Entry value.
     * @param writeVal Write value flag.
     * @param readVal Read value flag.
     */
    public void value(@Nullable V val, boolean writeVal, boolean readVal) {
        this.val.value(this.val.op(), val, writeVal, readVal);
    }

    /**
     * Sets read value if this tx entrty does not have write value yet.
     *
     * @param val Read value to set.
     */
    public void readValue(@Nullable V val) {
        this.val.value(this.val.op(), val, false, true);
    }

    /**
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     */
    public void addEntryProcessor(EntryProcessor<K, V, ?> entryProcessor, Object[] invokeArgs) {
        if (entryProcessorsCol == null)
            entryProcessorsCol = new LinkedList<>();

        entryProcessorsCol.add(new T2<EntryProcessor<K, V, ?>, Object[]>(entryProcessor, invokeArgs));

        // Must clear transform closure bytes since collection has changed.
        transformClosBytes = null;

        val.op(TRANSFORM);
    }

    /**
     * @return Collection of entry processors.
     */
    public Collection<T2<EntryProcessor<K, V, ?>, Object[]>> entryProcessors() {
        return entryProcessorsCol;
    }

    /**
     * @param val Value.
     * @return New value.
     */
    @SuppressWarnings("unchecked")
    public V applyEntryProcessors(V val) {
        for (T2<EntryProcessor<K, V, ?>, Object[]> t : entryProcessors()) {
            try {
                CacheInvokeEntry<K, V> invokeEntry = new CacheInvokeEntry<>(key, val);

                EntryProcessor processor = t.get1();

                processor.process(invokeEntry, t.get2());

                val = invokeEntry.getValue();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }

        return val;
    }

    /**
     * @param entryProcessorsCol Collection of entry processors.
     */
    public void entryProcessors(@Nullable Collection<T2<EntryProcessor<K, V, ?>, Object[]>> entryProcessorsCol) {
        this.entryProcessorsCol = entryProcessorsCol;

        // Must clear transform closure bytes since collection has changed.
        transformClosBytes = null;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return val.op();
    }

    /**
     * @param op Cache operation.
     */
    public void op(GridCacheOperation op) {
        val.op(op);
    }

    /**
     * @return {@code True} if read entry.
     */
    public boolean isRead() {
        return op() == READ;
    }

    /**
     * @param explicitVer Explicit version.
     */
    public void explicitVersion(GridCacheVersion explicitVer) {
        this.explicitVer = explicitVer;
    }

    /**
     * @return Explicit version.
     */
    public GridCacheVersion explicitVersion() {
        return explicitVer;
    }

    /**
     * @return DR version.
     */
    @Nullable public GridCacheVersion drVersion() {
        return drVer;
    }

    /**
     * @param drVer DR version.
     */
    public void drVersion(@Nullable GridCacheVersion drVer) {
        this.drVer = drVer;
    }

    /**
     * @return Put filters.
     */
    public IgnitePredicate<CacheEntry<K, V>>[] filters() {
        return filters;
    }

    /**
     * @param filters Put filters.
     */
    public void filters(IgnitePredicate<CacheEntry<K, V>>[] filters) {
        filterBytes = null;

        this.filters = filters;
    }

    /**
     * @return {@code True} if filters passed for fast-commit transactions.
     */
    public boolean filtersPassed() {
        return filtersPassed;
    }

    /**
     * @param filtersPassed {@code True} if filters passed for fast-commit transactions.
     */
    public void filtersPassed(boolean filtersPassed) {
        this.filtersPassed = filtersPassed;
    }

    /**
     * @return {@code True} if filters are set.
     */
    public boolean filtersSet() {
        return filtersSet;
    }

    /**
     * @param filtersSet {@code True} if filters are set and should not be replaced.
     */
    public void filtersSet(boolean filtersSet) {
        this.filtersSet = filtersSet;
    }

    /**
     * @param ctx Context.
     * @param transferExpiry {@code True} if expire policy should be marshalled.
     * @throws IgniteCheckedException If failed.
     */
    public void marshal(GridCacheSharedContext<K, V> ctx, boolean transferExpiry) throws IgniteCheckedException {
        // Do not serialize filters if they are null.
        if (depEnabled) {
            if (keyBytes == null)
                keyBytes = entry.getOrMarshalKeyBytes();

            if (transformClosBytes == null && entryProcessorsCol != null)
                transformClosBytes = CU.marshal(ctx, entryProcessorsCol);

            if (F.isEmptyOrNulls(filters))
                filterBytes = null;
            else if (filterBytes == null)
                filterBytes = CU.marshal(ctx, filters);
        }

        if (transferExpiry)
            transferExpiryPlc = expiryPlc != null && expiryPlc != this.ctx.expiry();

        val.marshal(ctx, context(), depEnabled);
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param near Near flag.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If un-marshalling failed.
     */
    public void unmarshal(GridCacheSharedContext<K, V> ctx, boolean near, ClassLoader clsLdr) throws IgniteCheckedException {
        if (this.ctx == null) {
            GridCacheContext<K, V> cacheCtx = ctx.cacheContext(cacheId);

            if (cacheCtx.isNear() && !near)
                cacheCtx = cacheCtx.near().dht().context();
            else if (!cacheCtx.isNear() && near)
                cacheCtx = cacheCtx.dht().near().context();

            this.ctx = cacheCtx;
        }

        if (depEnabled) {
            // Don't unmarshal more than once by checking key for null.
            if (key == null)
                key = ctx.marshaller().unmarshal(keyBytes, clsLdr);

            // Unmarshal transform closure anyway if it exists.
            if (transformClosBytes != null && entryProcessorsCol == null)
                entryProcessorsCol = ctx.marshaller().unmarshal(transformClosBytes, clsLdr);

            if (filters == null && filterBytes != null) {
                filters = ctx.marshaller().unmarshal(filterBytes, clsLdr);

                if (filters == null)
                    filters = CU.empty();
            }
        }

        val.unmarshal(this.ctx, clsLdr, depEnabled);
    }

    /**
     * @param expiryPlc Expiry policy.
     */
    public void expiry(@Nullable ExpiryPolicy expiryPlc) {
        this.expiryPlc = expiryPlc;
    }

    /**
     * @return Expiry policy.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(depEnabled);

        if (depEnabled) {
            U.writeByteArray(out, keyBytes);
            U.writeByteArray(out, transformClosBytes);
            U.writeByteArray(out, filterBytes);
        }
        else {
            out.writeObject(key);
            U.writeCollection(out, entryProcessorsCol);
            U.writeArray(out, filters);
        }

        out.writeInt(cacheId);

        val.writeTo(out);

        out.writeLong(ttl);
        out.writeLong(drExpireTime);

        CU.writeVersion(out, explicitVer);
        out.writeBoolean(grpLock);
        CU.writeVersion(out, drVer);

        out.writeObject(transferExpiryPlc ? new IgniteExternalizableExpiryPolicy(expiryPlc) : null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        depEnabled = in.readBoolean();

        if (depEnabled) {
            keyBytes = U.readByteArray(in);
            transformClosBytes = U.readByteArray(in);
            filterBytes = U.readByteArray(in);
        }
        else {
            key = (K)in.readObject();
            entryProcessorsCol = U.readCollection(in);
            filters = U.readEntryFilterArray(in);
        }

        cacheId = in.readInt();

        val.readFrom(in);

        ttl = in.readLong();
        drExpireTime = in.readLong();

        explicitVer = CU.readVersion(in);
        grpLock = in.readBoolean();
        drVer = CU.readVersion(in);

        expiryPlc = (ExpiryPolicy)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        ClassLoader clsLdr = getClass().getClassLoader();

        V val = value();

        // First of all check classes that may be loaded by class loader other than application one.
        return key != null && !clsLdr.equals(key.getClass().getClassLoader()) ?
            key.getClass() : val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteTxEntry.class, this,
            "keyBytesSize", keyBytes == null ? "null" : Integer.toString(keyBytes.length),
            "xidVer", tx == null ? "null" : tx.xidVersion());
    }

    /**
     * Auxiliary class to hold value, value-has-been-set flag, value update operation, value bytes.
     */
    private static class TxEntryValueHolder<K, V> {
        /** */
        @GridToStringInclude
        private V val;

        /** */
        @GridToStringExclude
        private byte[] valBytes;

        /** */
        @GridToStringInclude
        private GridCacheOperation op = NOOP;

        /** Flag indicating that value has been set for write. */
        private boolean hasWriteVal;

        /** Flag indicating that value has been set for read. */
        private boolean hasReadVal;

        /** Flag indicating that bytes were sent. */
        private boolean valBytesSent;

        /**
         * @param op Cache operation.
         * @param val Value.
         * @param hasWriteVal Write value presence flag.
         * @param hasReadVal Read value presence flag.
         */
        public void value(GridCacheOperation op, V val, boolean hasWriteVal, boolean hasReadVal) {
            if (hasReadVal && this.hasWriteVal)
                return;

            boolean clean = this.val != null;

            this.op = op;
            this.val = val;

            if (clean)
                valBytes = null;

            this.hasWriteVal = hasWriteVal || op == CREATE || op == UPDATE || op == DELETE;
            this.hasReadVal = hasReadVal || op == READ;
        }

        /**
         * @return {@code True} if has read or write value.
         */
        public boolean hasValue() {
            return hasWriteVal || hasReadVal;
        }

        /**
         * Gets stored value.
         *
         * @return Value.
         */
        public V value() {
            return val;
        }

        /**
         * @param val Stored value.
         */
        public void value(@Nullable V val) {
            boolean clean = this.val != null;

            this.val = val;

            if (clean)
                valBytes = null;
        }

        /**
         * Gets cache operation.
         *
         * @return Cache operation.
         */
        public GridCacheOperation op() {
            return op;
        }

        /**
         * Sets cache operation.
         *
         * @param op Cache operation.
         */
        public void op(GridCacheOperation op) {
            this.op = op;
        }

        /**
         * @return {@code True} if write value was set.
         */
        public boolean hasWriteValue() {
            return hasWriteVal;
        }

        /**
         * @return {@code True} if read value was set.
         */
        public boolean hasReadValue() {
            return hasReadVal;
        }

        /**
         * Sets value bytes.
         *
         * @param valBytes Value bytes to set.
         */
        public void valueBytes(@Nullable byte[] valBytes) {
            this.valBytes = valBytes;
        }

        /**
         * Gets value bytes.
         *
         * @return Value bytes.
         */
        public byte[] valueBytes() {
            return valBytes;
        }

        /**
         * @param sharedCtx Shared cache context.
         * @param ctx Cache context.
         * @param depEnabled Deployment enabled flag.
         * @throws IgniteCheckedException If marshaling failed.
         */
        public void marshal(GridCacheSharedContext<K, V> sharedCtx, GridCacheContext<K, V> ctx, boolean depEnabled)
            throws IgniteCheckedException {
            boolean valIsByteArr = val != null && val instanceof byte[];

            // Do not send write values to remote nodes.
            if (hasWriteVal && val != null && !valIsByteArr && valBytes == null &&
                (depEnabled || !ctx.isUnmarshalValues()))
                valBytes = CU.marshal(sharedCtx, val);

            valBytesSent = hasWriteVal && !valIsByteArr && valBytes != null && (depEnabled || !ctx.isUnmarshalValues());
        }

        /**
         * @param ctx Cache context.
         * @param ldr Class loader.
         * @param depEnabled Deployment enabled flag.
         * @throws IgniteCheckedException If unmarshalling failed.
         */
        public void unmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr, boolean depEnabled) throws IgniteCheckedException {
            if (valBytes != null && val == null && (ctx.isUnmarshalValues() || op == TRANSFORM || depEnabled))
                val = ctx.marshaller().unmarshal(valBytes, ldr);
        }

        /**
         * @param out Data output.
         * @throws IOException If failed.
         */
        public void writeTo(ObjectOutput out) throws IOException {
            out.writeBoolean(hasWriteVal);
            out.writeBoolean(valBytesSent);

            if (hasWriteVal) {
                if (valBytesSent)
                    U.writeByteArray(out, valBytes);
                else {
                    if (val != null && val instanceof byte[]) {
                        out.writeBoolean(true);

                        U.writeByteArray(out, (byte[])val);
                    }
                    else {
                        out.writeBoolean(false);

                        out.writeObject(val);
                    }
                }
            }

            out.writeInt(op.ordinal());
        }

        /**
         * @param in Data input.
         * @throws IOException If failed.
         * @throws ClassNotFoundException If failed.
         */
        public void readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
            hasWriteVal = in.readBoolean();
            valBytesSent = in.readBoolean();

            if (hasWriteVal) {
                if (valBytesSent)
                    valBytes = U.readByteArray(in);
                else
                    val = in.readBoolean() ? (V)U.readByteArray(in) : (V)in.readObject();
            }

            op = fromOrdinal(in.readInt());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "[op=" + op +", val=" + val + ", valBytesLen=" + (valBytes == null ? 0 : valBytes.length) + ']';
        }
    }
}
