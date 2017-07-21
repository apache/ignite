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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Generic cache request.
 */
public class GridClientCacheRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

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
        PREPEND;

        /** Enumerated values. */
        private static final GridCacheOperation[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static GridCacheOperation fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /** Requested cache operation. */
    private GridCacheOperation op;

    /** Cache name. */
    private String cacheName;

    /** Key */
    private Object key;

    /** Value (expected value for CAS). */
    private Object val;

    /** New value for CAS. */
    private Object val2;

    /** Keys and values for put all, get all, remove all operations. */
    private Map<Object, Object> vals;

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
     * @return Values map for batch operations.
     */
    public Map<Object, Object> values() {
        return vals;
    }

    /**
     * @param vals Values map for batch operations.
     */
    public void values(Map<Object, Object> vals) {
        this.vals = vals;
    }

    /**
     * @param keys Keys collection
     */
    public void keys(Iterable<Object> keys) {
        vals = new HashMap<>();

        for (Object k : keys)
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
    @SuppressWarnings("deprecation")
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
    @SuppressWarnings("deprecation")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        op = GridCacheOperation.fromOrdinal(in.readByte());

        cacheName = U.readString(in);

        key = in.readObject();
        val = in.readObject();
        val2 = in.readObject();

        vals = U.readMap(in);

        cacheFlagsOn = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [op=" + op + ", key=" + key + ", val=" + val +
            ", val2=" + val2 + ", vals=" + vals + ", cacheFlagsOn=" + cacheFlagsOn + "]";
    }
}