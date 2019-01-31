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
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Table row implementation based on {@link GridQueryTypeDescriptor}.
 */
public class GridH2FullRowReadOnly extends GridH2SearchRowAdapter {
    /** */
    public static final int DEFAULT_COLUMNS_COUNT = 2;

    /** Key column. */
    public static final int KEY_COL = 0;

    /** Value column. */
    public static final int VAL_COL = 1;

    /** */
    private final GridH2RowDescriptor desc;

    /** Key. */
    private final Object key;

    /** Value. */
    private final Object val;

    /** Link. */
    private final long link;

    /** Cache ID. */
    private final int cacheId;

    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param key Key.
     * @param val Value.
     * @param link Link.
     * @param cacheId Cache ID.
     */
    public GridH2FullRowReadOnly(GridH2RowDescriptor desc, Object key, Object val, long link, int cacheId) {
        this.desc = desc;
        this.key = key;
        this.val = val;
        this.link = link;
        this.cacheId = cacheId;
    }

    /**
     * @return Link.
     */
    public long link() {
        return link;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return DEFAULT_COLUMNS_COUNT + desc.fieldsCount();
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        switch (col) {
            case KEY_COL:
                return keyWrapped();

            case VAL_COL:
                return valueWrapped();

            default:
                if (desc.isKeyAliasColumn(col))
                    return keyWrapped();
                else if (desc.isValueAliasColumn(col))
                    return valueWrapped();

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
        Object res = desc.columnValue(key, val, col);

        return res == null ? ValueNull.INSTANCE : wrap(res, desc.fieldType(col));
    }

    /**
     * @return Wrapped key value.
     */
    private Value keyWrapped() {
        return wrap(key, desc.keyType());
    }

    /**
     * @return Wrapped value value.
     */
    private Value valueWrapped() {
        return wrap(val, desc.valueType());
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
            return H2Utils.wrap(desc.indexing().objectContext(), val, type);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wrap object into H2 Value.", e);
        }
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
    @Override public boolean indexSearchRow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Value v = keyWrapped();
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = valueWrapped();
        sb.a(", val: ").a(v == null ? "nil" : v.getString());

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
}