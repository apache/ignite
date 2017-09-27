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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table row implementation based on {@link GridQueryTypeDescriptor}.
 */
public class GridH2KeyValueRowOnheap extends GridH2Row {
    /** */
    public static final int DEFAULT_COLUMNS_COUNT = 3;

    /** Key column. */
    public static final int KEY_COL = 0;

    /** Value column. */
    public static final int VAL_COL = 1;

    /** Version column. */
    public static final int VER_COL = 2;

    /** */
    protected final GridH2RowDescriptor desc;

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    protected long expirationTime;

    /** */
    private Value key;

    /** */
    private volatile Value val;

    /** */
    private Value[] valCache;

    /** */
    private Value version;

    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param key Key.
     * @param keyType Key type.
     * @param val Value.
     * @param valType Value type.
     * @param expirationTime Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2KeyValueRowOnheap(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val,
        int valType, GridCacheVersion ver, long expirationTime) throws IgniteCheckedException {
        this.desc = desc;
        this.expirationTime = expirationTime;

        setValue(KEY_COL, desc.wrap(key, keyType));

        if (val != null) // We remove by key only, so value can be null here.
            setValue(VAL_COL, desc.wrap(val, valType));

        if (ver != null)
            setValue(VER_COL, desc.wrap(ver, Value.JAVA_OBJECT));
    }

    /** {@inheritDoc} */
    @Override public Value[] getValueList() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expirationTime;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return DEFAULT_COLUMNS_COUNT + desc.fieldsCount();
    }

    /**
     * @param col Column index.
     * @return Value if exists.
     */
    protected final Value peekValue(int col) {
        if (col == KEY_COL)
            return key;

        if (col == VAL_COL)
            return val;

        assert col == VER_COL;

        return version;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        Value[] vCache = valCache;

        if (vCache != null) {
            Value v = vCache[col];

            if (v != null)
                return v;
        }

        Value v;

        if (desc.isValueColumn(col)) {
            v = peekValue(VAL_COL);

            return v;
        }
        else if (desc.isKeyColumn(col)) {
            v = peekValue(KEY_COL);

            assert v != null;

            return v;
        }
        else if (col == VER_COL)
            return version;

        col -= DEFAULT_COLUMNS_COUNT;

        assert col >= 0;

        Value key = getValue(KEY_COL);
        Value val = getValue(VAL_COL);

        assert key != null;
        assert val != null;

        Object res = desc.columnValue(key.getObject(), val.getObject(), col);

        if (res == null)
            v = ValueNull.INSTANCE;
        else {
            try {
                v = desc.wrap(res, desc.fieldType(col));
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        if (vCache != null)
            vCache[col + DEFAULT_COLUMNS_COUNT] = v;

        return v;
    }

    /**
     * @param valCache Value cache.
     */
    public void valuesCache(Value[] valCache) {
        if (valCache != null) {
            desc.initValueCache(valCache, key, val, version);
        }

        this.valCache = valCache;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Value v = peekValue(KEY_COL);
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = peekValue(VAL_COL);
        sb.a(", val: ").a(v == null ? "nil" : v.getString());

        v = peekValue(VER_COL);
        sb.a(", ver: ").a(v == null ? "nil" : v.getString());

        sb.a(" ][ ");

        if (v != null) {
            for (int i = DEFAULT_COLUMNS_COUNT, cnt = getColumnCount(); i < cnt; i++) {
                v = getValue(i);

                if (i != DEFAULT_COLUMNS_COUNT)
                    sb.a(", ");

                if (!desc.isKeyValueOrVersionColumn(i))
                    sb.a(v == null ? "nil" : v.getString());
            }
        }

        sb.a(" ]");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public void setKeyAndVersion(SearchRow old) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void setKey(long key) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public Row getCopy() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void setDeleted(boolean deleted) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public long getKey() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void setSessionId(int sesId) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void setVersion(int ver) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        if (desc.isValueColumn(idx))
            val = v;
        else if (idx == VER_COL)
            version = v;
        else {
            assert desc.isKeyColumn(idx) : idx + " " + v;

            key = v;
        }
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        throw new IllegalStateException();
    }
}