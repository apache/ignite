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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;

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
    private Value key;

    /** */
    private volatile Value val;

    /** */
    private Value[] valCache;

    /** */
    private Value ver;

    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param row Row.
     * @param keyType Key type.
     * @param valType Value type.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2KeyValueRowOnheap(GridH2RowDescriptor desc,
        CacheDataRow row,
        int keyType,
        int valType) throws IgniteCheckedException {
        super(row);

        this.desc = desc;

        this.key = desc.wrap(row.key(), keyType);

        if (row.value() != null)
            this.val = desc.wrap(row.value(), valType);

        if (row.version() != null)
            this.ver = desc.wrap(row.version(), Value.JAVA_OBJECT);
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return DEFAULT_COLUMNS_COUNT + desc.fieldsCount();
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        switch (col) {
            case KEY_COL:
                return key;

            case VAL_COL:
                return val;

            case VER_COL:
                return ver;

            default:
                if (desc.isKeyAliasColumn(col))
                    return key;
                else if (desc.isValueAliasColumn(col))
                    return val;

                return getValue0(col - DEFAULT_COLUMNS_COUNT);
        }
    }

    /**
     * Get real column value.
     *
     * @param col Adjusted column index (without default columns).
     * @return Value.
     */
    private Value getValue0(int col) {
        Value v = getCached(col);

        if (v != null)
            return v;

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

        setCached(col, v);

        return v;
    }

    /**
     * Prepare values cache.
     */
    public void prepareValuesCache() {
        this.valCache = new Value[desc.fieldsCount()];
    }

    /**
     * Clear values cache.
     */
    public void clearValuesCache() {
        this.valCache = null;
    }

    /**
     * Get cached value (if any).
     *
     * @param colIdx Column index.
     * @return Value.
     */
    private Value getCached(int colIdx) {
        return valCache != null ? valCache[colIdx] : null;
    }

    /**
     * Set cache value.
     *
     * @param colIdx Column index.
     * @param val Value.
     */
    private void setCached(int colIdx, Value val) {
        if (valCache != null)
            valCache[colIdx] = val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Value v = key;
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = val;
        sb.a(", val: ").a(v == null ? "nil" : v.getString());

        v = ver;
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
    @Override public void setKey(long key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public final int hashCode() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        throw new UnsupportedOperationException();
    }
}