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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Table row implementation based on {@link GridQueryTypeDescriptor}.
 */
public class H2CacheRow extends H2Row implements CacheDataRow {
    /** H2 row descriptor. */
    private final GridH2RowDescriptor desc;

    /** Cache row. */
    private final CacheDataRow row;

    /** */
    private Value[] valCache;

    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param row Row.
     */
    public H2CacheRow(GridH2RowDescriptor desc, CacheDataRow row) {
        this.desc = desc;
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        if (removedRow())
            return 1;

        return QueryUtils.DEFAULT_COLUMNS_COUNT + desc.fieldsCount();
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        if (removedRow()) {
            assert col == 0 : col;

            return keyWrapped();
        }

        if (desc.isKeyColumn(col))
            return keyWrapped();
        else if (desc.isValueColumn(col))
            return valueWrapped();

        return getValue0(col - QueryUtils.DEFAULT_COLUMNS_COUNT);
    }

    /**
     * Get real field value.
     *
     * @param fieldIdx Field index.
     * @return Value.
     */
    private Value getValue0(int fieldIdx) {
        Value v = getCached(fieldIdx);

        if (v != null)
            return v;

        Object res = desc.getFieldValue(row.key(), row.value(), fieldIdx);

        v = res == null ? ValueNull.INSTANCE : wrap(res, desc.fieldType(fieldIdx));

        setCached(fieldIdx, v);

        return v;
    }

    /**
     * Prepare values cache.
     */
    public void prepareValuesCache() {
        valCache = new Value[desc.fieldsCount()];
    }

    /**
     * Clear values cache.
     */
    public void clearValuesCache() {
        valCache = null;
    }

    /**
     * Get cached value (if any).
     *
     * @param colIdx Column index.
     * @return Value.
     */
    public Value getCached(int colIdx) {
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

    /**
     * @return Wrapped key value.
     */
    private Value keyWrapped() {
        return wrap(row.key(), desc.keyType());
    }

    /**
     * @return Wrapped value value.
     */
    private Value valueWrapped() {
        return wrap(row.value(), desc.valueType());
    }

    /**
     * Wrap the given object into H2 value.
     *
     * @param val Value.
     * @param type Type.
     * @return wrapped value.
     */
    private Value wrap(Object val, int type) {
        try {
            return H2Utils.wrap(desc.context().kernalContext().query().objectContext(), val, type);
        }
        catch (ClassCastException e) {
            throw new IgniteSQLException("Failed to wrap object into H2 Value. " + e.getMessage(),
                IgniteQueryErrorCode.FIELD_TYPE_MISMATCH, e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wrap object into H2 Value.", e);
        }
    }

    /**
     * @return Cache data row.
     */
    public CacheDataRow getRow() {
        return row;
    }

    /**
     * @return {@code True} if this is removed row (doesn't have value).
     */
    private boolean removedRow() {
        return row.value() == null;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return row.key();
    }

    /** {@inheritDoc} */
    @Override public void key(KeyCacheObject key) {
        row.key(key);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return row.value();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return row.version();
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return row.partition();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return row.expireTime();
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return row.link();
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        row.link(link);
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return row.hash();
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return row.cacheId();
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return false;
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
    @Override public int hashCode() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Value v = keyWrapped();
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = valueWrapped();
        sb.a(", val: ").a(v == null ? "nil" : (S.includeSensitive() ? v.getString() :
            "Data hidden due to " + IGNITE_TO_STRING_INCLUDE_SENSITIVE + " flag."));

        sb.a(" ][ ");

        if (v != null) {
            for (int i = QueryUtils.DEFAULT_COLUMNS_COUNT, cnt = getColumnCount(); i < cnt; i++) {
                if (i != QueryUtils.DEFAULT_COLUMNS_COUNT)
                    sb.a(", ");

                try {
                    v = getValue(i);

                    if (!(desc.isKeyColumn(i) || desc.isValueColumn(i)))
                        sb.a(v == null ? "nil" : (S.includeSensitive() ? v.getString() : "data hidden"));
                }
                catch (Exception e) {
                    sb.a("<value skipped on error: " + e.getMessage() + '>');
                }
            }
        }

        sb.a(" ]");

        return sb.toString();
    }

    /**
     * @return H2 row descriptor.
     */
    public GridQueryRowDescriptor getDesc() {
        return desc;
    }
}
