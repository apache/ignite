/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.request;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

public class GridRestCacheRequest extends GridRestRequest {
    /** Cache name. */
    private String cacheName;

    /** Key. */
    private Object key;

    /** Value (expected value for CAS). */
    private Object val;

    /** New value for CAS. */
    private Object val2;

    /** Keys and values for put all, get all, remove all operations. */
    private Map<Object, Object> vals;

    /** Bit map of cache flags to be enabled on cache projection. */
    private int cacheFlags;

    /** Expiration time. */
    private Long ttl;

    /** Value to add/subtract. */
    private Long delta;

    /** Initial value for increment and decrement commands. */
    private Long init;

    /**
     * @return Cache name, or {@code null} if not set.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(Object key) {
        this.key = key;
    }

    /**
     * @return Value 1.
     */
    public Object value() {
        return val;
    }

    /**
     * @param val Value 1.
     */
    public void value(Object val) {
        this.val = val;
    }

    /**
     * @return Value 2.
     */
    public Object value2() {
        return val2;
    }

    /**
     * @param val2 Value 2.
     */
    public void value2(Object val2) {
        this.val2 = val2;
    }

    /**
     * @return Keys and values for put all, get all, remove all operations.
     */
    public Map<Object, Object> values() {
        return vals;
    }

    /**
     * @param vals Keys and values for put all, get all, remove all operations.
     */
    public void values(Map<Object, Object> vals) {
        this.vals = vals;
    }

    /**
     * @param cacheFlags Bit representation of cache flags.
     */
    public void cacheFlags(int cacheFlags) {
        this.cacheFlags = cacheFlags;
    }

    /**
     * @return Bit representation of cache flags.
     */
    public int cacheFlags() {
        return cacheFlags;
    }

    /**
     * @return Expiration time.
     */
    public Long ttl() {
        return ttl;
    }

    /**
     * @param ttl Expiration time.
     */
    public void ttl(Long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return Delta for increment and decrement commands.
     */
    public Long delta() {
        return delta;
    }

    /**
     * @param delta Delta for increment and decrement commands.
     */
    public void delta(Long delta) {
        this.delta = delta;
    }

    /**
     * @return Initial value for increment and decrement commands.
     */
    public Long initial() {
        return init;
    }

    /**
     * @param init Initial value for increment and decrement commands.
     */
    public void initial(Long init) {
        this.init = init;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestCacheRequest.class, this, super.toString());
    }
}
