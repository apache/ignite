/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Swap entry.
 */
public class GridCacheSwapEntryImpl<V> implements GridCacheSwapEntry<V>, Externalizable, GridPortableMarshalAware {
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
    public GridCacheSwapEntryImpl() {
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
    public GridCacheSwapEntryImpl(int keyHash,
        byte[] valBytes,
        boolean valIsByteArr,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        GridUuid keyClsLdrId,
        @Nullable GridUuid valClsLdrId) {
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

    /** {@inheritDoc} */
    @Override public int keyHash() {
        return keyHash;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes() {
        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(V val) {
        this.val = val;

        if (val instanceof byte[])
            valBytes = null;
    }

    /** {@inheritDoc} */
    @Override public boolean valueIsByteArray() {
        return valIsByteArr;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid keyClassLoaderId() {
        return keyClsLdrId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid valueClassLoaderId() {
        return valClsLdrId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridBiTuple<Long, Integer> offheapPointer() {
        return null;
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
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        GridPortableRawWriter raw = writer.rawWriter();

        raw.writeLong(ttl);
        raw.writeLong(expireTime);

        raw.writeInt(keyHash);

        writeVersion(raw, ver, true);

        raw.writeByteArray(valBytes);
        raw.writeBoolean(valIsByteArr);

        writeGridUuid(raw, keyClsLdrId);
        writeGridUuid(raw, valClsLdrId);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        GridPortableRawReader raw = reader.rawReader();

        valBytes = raw.readByteArray();
        valIsByteArr = raw.readBoolean();

        ttl = raw.readLong();
        expireTime = raw.readLong();

        keyHash = raw.readInt();

        ver = (GridCacheVersion)raw.readObject();

        keyClsLdrId = readGridUuid(raw);
        valClsLdrId = readGridUuid(raw);
    }

    /**
     * @param raw Writer.
     * @param uid UUID.
     */
    private void writeGridUuid(GridPortableRawWriter raw, @Nullable GridUuid uid) {
        raw.writeBoolean(uid != null);

        if (uid != null) {
            raw.writeLong(uid.globalId().getMostSignificantBits());
            raw.writeLong(uid.globalId().getLeastSignificantBits());

            raw.writeLong(uid.localId());
        }
    }

    /**
     * @param raw Writer.
     * @param ver Version.
     * @param checkEx If {@code true} checks if version is {@link GridCacheVersionEx}.
     */
    private void writeVersion(GridPortableRawWriter raw, GridCacheVersion ver, boolean checkEx) {
        boolean verEx = false;

        if (checkEx) {
            verEx = ver instanceof GridCacheVersionEx;

            raw.writeBoolean(verEx);
        }

        raw.writeInt(ver.topologyVersion());
        raw.writeInt(ver.nodeOrderAndDrIdRaw());
        raw.writeLong(ver.globalTime());
        raw.writeLong(ver.order());

        if (verEx)
            writeVersion(raw, ver.drVersion(), false);
    }

    /**
     * @param raw Reader.
     * @return Version.
     */
    @SuppressWarnings("IfMayBeConditional")
    private GridCacheVersion readVersion(GridPortableRawReader raw) {
        boolean verEx = raw.readBoolean();

        if (verEx) {
            return new GridCacheVersionEx(raw.readInt(), raw.readInt(), raw.readLong(), raw.readLong(),
                new GridCacheVersion(raw.readInt(), raw.readInt(), raw.readLong(), raw.readLong()));
        }
        else
            return new GridCacheVersion(raw.readInt(), raw.readInt(), raw.readLong(), raw.readLong());
    }

    /**
     * @param raw Reader.
     * @return Read UUID.
     */
    @Nullable private GridUuid readGridUuid(GridPortableRawReader raw) {
        if (raw.readBoolean()) {
            long most = raw.readLong();
            long least = raw.readLong();

            UUID globalId = GridUuidCache.onGridUuidRead(new UUID(most, least));

            long locId = raw.readLong();

            return new GridUuid(globalId, locId);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSwapEntryImpl.class, this);
    }
}
