/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Swap entry.
 */
public class GridCacheSwapEntry<V> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key hash. */
    private int keyHash;

    /** Value bytes. */
    private byte[] valBytes;

    /** Value. */
    private V val;

    /** Falg indicating that value is byte array, so valBytes should not be unmarshalled. */
    private boolean valIsByteArr;

    /** Class loader ID. */
    private GridUuid keyClsLdrId;

    /** Class loader ID. */
    private GridUuid valClsLdrId;

    /** Version. */
    private GridCacheVersion ver;

    /** Time to live. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Empty constructor.
     */
    public GridCacheSwapEntry() {
        // No-op.
    }

    /**
     * @param keyHash Key hash.
     * @param valBytes Value.
     * @param valIsByteArr Whether value of this entry is byte array.
     * @param ver Version.
     * @param ttl Entry time to live.
     * @param expireTime Expire time.
     * @param keyClsLdrId Class loader ID for entry key (can be {@code null} for local class loader).
     * @param valClsLdrId Class loader ID for entry value (can be {@code null} for local class loader).
     */
    public GridCacheSwapEntry(int keyHash, byte[] valBytes, boolean valIsByteArr, GridCacheVersion ver, long ttl,
        long expireTime, GridUuid keyClsLdrId, @Nullable GridUuid valClsLdrId) {
        assert ver != null;

        this.keyHash = keyHash;
        this.valBytes = valBytes;
        this.valIsByteArr = valIsByteArr;
        this.ver = ver;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.valClsLdrId = valClsLdrId;
        this.keyClsLdrId = keyClsLdrId;
    }

    /**
     * @return Key hash.
     */
    public int keyHash() {
        return keyHash;
    }

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes() {
        return valBytes;
    }

    /**
     * @return Value.
     */
    public V value() {
        return val;
    }

    /**
     * @param val Value.
     */
    void value(V val) {
        this.val = val;

        if (val instanceof byte[])
            valBytes = null;
    }

    /**
     * @return Whether value is byte array.
     */
    public boolean valueIsByteArray() {
        return valIsByteArr;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     * @return Class loader ID for entry key ({@code null} for local class loader).
     */
    @Nullable public GridUuid keyClassLoaderId() {
        return keyClsLdrId;
    }

    /**
     * @return Class loader ID for entry value ({@code null} for local class loader).
     */
    @Nullable public GridUuid valueClassLoaderId() {
        return valClsLdrId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, keyClsLdrId);
        U.writeGridUuid(out, valClsLdrId);

        U.writeByteArray(out, valBytes);

        out.writeBoolean(valIsByteArr);

        CU.writeVersion(out, ver);

        out.writeLong(ttl);
        out.writeLong(expireTime);

        out.writeInt(keyHash);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        keyClsLdrId = U.readGridUuid(in);
        valClsLdrId = U.readGridUuid(in);

        valBytes = U.readByteArray(in);

        valIsByteArr = in.readBoolean();

        ver = CU.readVersion(in);

        ttl = in.readLong();
        expireTime = in.readLong();

        keyHash = in.readInt();

        assert ver != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSwapEntry.class, this);
    }
}
