/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Generic cache request.
 */
public class GridClientCacheRequest<K, V> extends GridClientAbstractMessage {
    private static final long serialVersionUID = -6606433531891052547L;

    /**
     * Available cache operations.
     */
    @SuppressWarnings("PublicInnerClass")
    public enum GridCacheOperation {
        /** Cache put. */
        PUT,

        /** Cache put all. */
        PUT_ALL,

        /** Cache get. */
        GET,

        /** Cache get all. */
        GET_ALL,

        /** Cache remove. */
        RMV,

        /** Cache remove all. */
        RMV_ALL,

        /** Cache replace (put only if exists).  */
        REPLACE,

        /** Cache compare and set. */
        CAS,

        /** Cache metrics request. */
        METRICS,

        /** Append requested value to already cached one. */
        APPEND,

        /** Prepend requested value to already cached one. */
        PREPEND
    }

    /** Requested cache operation. */
    private GridCacheOperation op;

    /** Cache name. */
    private String cacheName;

    /** Key */
    private K key;

    /** Value (expected value for CAS). */
    private V val;

    /** New value for CAS. */
    private V val2;

    /** Keys and values for put all, get all, remove all operations. */
    private Map<K, V> vals;

    /** Bit map of cache flags to be enabled on cache projection */
    private int cacheFlagsOn;

    /**
     * Constructor for {@link Externalizable}.
     */
    public GridClientCacheRequest() {
        // No-op.
    }

    /**
     * Creates grid cache request.
     *
     * @param op Requested operation.
     */
    public GridClientCacheRequest(GridCacheOperation op) {
        this.op = op;
    }

    /**
     * @return Requested operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name, or {@code null} if not set.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Gets cache name.
     *
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Key.
     */
    public K key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(K key) {
        this.key = key;
    }

    /**
     * @return Value 1.
     */
    public V value() {
        return val;
    }

    /**
     * @param val Value 1.
     */
    public void value(V val) {
        this.val = val;
    }

    /**
     * @return Value 2.
     */
    public V value2() {
        return val2;
    }

    /**
     * @param val2 Value 2.
     */
    public void value2(V val2) {
        this.val2 = val2;
    }

    /**
     * @return Values map for batch operations.
     */
    public Map<K, V> values() {
        return vals;
    }

    /**
     * @param vals Values map for batch operations.
     */
    public void values(Map<K, V> vals) {
        this.vals = vals;
    }

    /**
     * @param keys Keys collection
     */
    public void keys(Iterable<K> keys) {
        vals = new HashMap<>();

        for (K k : keys)
            vals.put(k, null);
    }

    /**
     * Set cache flags bit map.
     *
     * @param cacheFlagsOn Bit representation of cache flags.
     */
    public void cacheFlagsOn(int cacheFlagsOn) {
        this.cacheFlagsOn = cacheFlagsOn;
    }

    /**
     * Get cache flags bit map.
     * @return Bit representation of cache flags.
     */
    public int cacheFlagsOn() {
        return cacheFlagsOn;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeEnum(out, op);

        U.writeString(out, cacheName);

        out.writeObject(key);
        out.writeObject(val);
        out.writeObject(val2);

        U.writeMap(out, vals);

        out.writeInt(cacheFlagsOn);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        op = U.readEnum(in, GridCacheOperation.class);

        cacheName = U.readString(in);

        key = (K)in.readObject();
        val = (V)in.readObject();
        val2 = (V)in.readObject();

        vals = U.readMap(in);

        cacheFlagsOn = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [op=" + op + ", key=" + key + ", val=" + val +
            ", val2=" + val2 + ", vals=" + vals + ", cacheFlagsOn=" + cacheFlagsOn + "]";
    }
}
