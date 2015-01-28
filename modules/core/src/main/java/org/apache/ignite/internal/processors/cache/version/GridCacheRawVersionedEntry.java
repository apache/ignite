package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridCacheRawVersionedEntry<K, V> implements GridCacheVersionedEntry<K, V>, GridCacheVersionable,
    Map.Entry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key. */
    private K key;

    /** Key bytes. */
    private byte[] keyBytes;

    /** Value. */
    private V val;

    /** Value bytes. */
    private byte[] valBytes;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * {@code Externalizable) support.
     */
    public GridCacheRawVersionedEntry() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param expireTime Expire time.
     * @param ttl TTL.
     * @param ver Version.
     */
    public GridCacheRawVersionedEntry(K key, @Nullable byte[] keyBytes, @Nullable V val, @Nullable byte[] valBytes,
        long ttl, long expireTime, GridCacheVersion ver) {
        this.key = key;
        this.keyBytes = keyBytes;
        this.val = val;
        this.valBytes = valBytes;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public K key() {
        assert key != null : "Entry is being improperly processed.";

        return key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val;
    }

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes() {
        return valBytes;
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
    @Override public byte dataCenterId() {
        return ver.dataCenterId();
    }

    /** {@inheritDoc} */
    @Override public int topologyVersion() {
        return ver.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return ver.order();
    }

    /** {@inheritDoc} */
    @Override public long globalTime() {
        return ver.globalTime();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * Perform internal unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshal(IgniteMarshaller marsh) throws IgniteCheckedException {
        unmarshalKey(marsh);

        if (valBytes != null && val == null)
            val = marsh.unmarshal(valBytes, null);
    }

    /**
     * Perform internal key unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshalKey(IgniteMarshaller marsh) throws IgniteCheckedException {
        if (key == null)
            key = marsh.unmarshal(keyBytes, null);
    }

    /**
     * Perform internal marshal of this entry before it will be serialized.
     *
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void marshal(IgniteMarshaller marsh) throws IgniteCheckedException {
        if (keyBytes == null)
            keyBytes = marsh.marshal(key);

        if (valBytes == null && val != null)
            valBytes = marsh.marshal(val);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert keyBytes != null;

        U.writeByteArray(out, keyBytes);
        U.writeByteArray(out, valBytes);

        out.writeLong(ttl);

        if (ttl != 0)
            out.writeLong(expireTime);

        out.writeObject(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        keyBytes = U.readByteArray(in);
        valBytes = U.readByteArray(in);

        ttl = in.readLong();

        if (ttl != 0)
            expireTime = in.readLong();

        ver = (GridCacheVersion)in.readObject();

        assert keyBytes != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheRawVersionedEntry.class, this, "keyBytesLen",
            keyBytes != null ? keyBytes.length : "n/a", "valBytesLen", valBytes != null ? valBytes.length : "n/a");
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key();
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return value();
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        throw new UnsupportedOperationException();
    }
}
