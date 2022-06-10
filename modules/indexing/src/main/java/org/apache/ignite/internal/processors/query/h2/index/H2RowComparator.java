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

package org.apache.ignite.internal.processors.query.h2.index;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCompartorImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Provide H2 logic of keys comparation.
 */
public class H2RowComparator extends IndexRowCompartorImpl {
    /** Cache context. */
    private final CacheObjectContext coctx;

    /** Table. */
    private final GridH2Table table;

    /** Ignite H2 session. */
    private final SessionInterface ses;

    /** */
    public H2RowComparator(GridH2Table table, IndexKeyTypeSettings keyTypeSettings) {
        super(keyTypeSettings);

        this.table = table;

        coctx = table.rowDescriptor().context().cacheObjectContext();
        ses = table.tableDescriptor().indexing().connections().jdbcConnection().getSession();
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, InlineIndexKeyType type) {
        int cmp = super.compareKey(pageAddr, off, maxSize, key, type);

        if (cmp != COMPARE_UNSUPPORTED)
            return cmp;

        IndexKeyType objType = key == NullIndexKey.INSTANCE ? type.type() : key.type();

        int highOrder = Value.getHigherOrder(type.type().code(), objType.code());

        // H2 supports comparison between different types after casting them to single type.
        if (highOrder != objType.code() && highOrder == type.type().code()) {
            Value va = DataType.convertToValue(ses, key.key(), highOrder);
            va = va.convertTo(highOrder);

            IndexKey objHighOrder = IndexKeyFactory.wrap(
                va.getObject(), highOrder, coctx, keyTypeSettings);

            InlineIndexKeyType highType = InlineIndexKeyTypeRegistry.get(objHighOrder,
                IndexKeyType.forCode(highOrder), keyTypeSettings);

            // The only way to invoke inline comparison again.
            if (highType != null)
                return ((NullableInlineIndexKeyType)highType).compare0(pageAddr, off, objHighOrder);
        }

        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int compareRow(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException {
        int cmp = super.compareRow(left, right, idx);

        if (cmp != COMPARE_UNSUPPORTED)
            return cmp;

        int ltype, rtype;

        Object lobject = left.key(idx).key();
        Object robject = right.key(idx).key();

        // Side of comparison can be set by user in query with type that different for specified schema.
        if (left.indexSearchRow())
            ltype = DataType.getTypeFromClass(lobject.getClass());
        else
            ltype = left.rowHandler().indexKeyDefinitions().get(idx).idxType().code();

        if (right.indexSearchRow())
            rtype = DataType.getTypeFromClass(robject.getClass());
        else
            rtype = right.rowHandler().indexKeyDefinitions().get(idx).idxType().code();

        int c = compareValues(wrap(lobject, ltype), wrap(robject, rtype));

        return Integer.signum(c);
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexKey left, IndexKey right) throws IgniteCheckedException {
        int cmp = super.compareKey(left, right);

        if (cmp != COMPARE_UNSUPPORTED)
            return cmp;

        int ltype = DataType.getTypeFromClass(left.key().getClass());
        int rtype = DataType.getTypeFromClass(right.key().getClass());

        int c = compareValues(wrap(left.key(), ltype), wrap(right.key(), rtype));

        return Integer.signum(c);
    }

    /** */
    private Value wrap(Object val, int type) throws IgniteCheckedException {
        return H2Utils.wrap(coctx, val, type);
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) throws IgniteCheckedException {
        try {
            return table.compareTypeSafe(v1, v2);
        }
        catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }

    /**
     * Is value convertible from one class to another.
     */
    private boolean isConvertible(Class<?> from, Class<?> to) {
        if (to == Byte.class)
            return from == Boolean.class;
        else if (to == Short.class)
            return from == Boolean.class || from == Byte.class;
        else if (to == Integer.class)
            return from == Boolean.class || from == Byte.class || from == Short.class;
        else if (to == Long.class)
            return from == Boolean.class || from == Byte.class || from == Short.class || from == Integer.class;
        else if (BigInteger.class.isAssignableFrom(to)) {
            return from == Boolean.class || from == Byte.class || from == Short.class || from == Integer.class ||
                from == Long.class;
        }
        else if (to == Float.class) {
            return from == Boolean.class || from == Byte.class || from == Short.class || from == Integer.class ||
                from == Long.class || BigInteger.class.isAssignableFrom(from);
        }
        else if (to == Double.class) {
            return from == Boolean.class || from == Byte.class || from == Short.class || from == Integer.class ||
                from == Long.class || BigInteger.class.isAssignableFrom(from) || from == Float.class;
        }
        else if (BigDecimal.class.isAssignableFrom(to)) {
            return from == Boolean.class || from == Byte.class || from == Short.class || from == Integer.class ||
                from == Long.class || BigInteger.class.isAssignableFrom(from) || from == Float.class ||
                from == Double.class;
        }

        return false;
    }

    /**
     * Converts value to class if possible.
     */
    private Object convert(Object val, Class<?> to) {
        Class<?> from = val.getClass();

        if (to == Byte.class) {
            if (from == Boolean.class)
                return (byte)(val == Boolean.TRUE ? 1 : 0);
        }
        else if (to == Short.class) {
            if (from == Boolean.class)
                return (short)(val == Boolean.TRUE ? 1 : 0);
            else if (from == Byte.class)
                return Short.valueOf((Byte)val);
        }
        else if (to == Integer.class) {
            if (from == Boolean.class)
                return val == Boolean.TRUE ? 1 : 0;
            else if (from == Byte.class)
                return Integer.valueOf((Byte)val);
            else if (from == Short.class)
                return Integer.valueOf((Short)val);
        }
        else if (to == Long.class) {
            if (from == Boolean.class)
                return val == Boolean.TRUE ? 1 : 0;
            else if (from == Byte.class)
                return Long.valueOf((Byte)val);
            else if (from == Short.class)
                return Long.valueOf((Short)val);
            else if (from == Integer.class)
                return Long.valueOf((Integer)val);
        }
        else if (BigInteger.class.isAssignableFrom(to)) {
            if (from == Boolean.class)
                return val == Boolean.TRUE ? BigInteger.ONE : BigInteger.ZERO;
            else if (from == Byte.class)
                return BigInteger.valueOf((Byte)val);
            else if (from == Short.class)
                return BigInteger.valueOf((Short)val);
            else if (from == Integer.class)
                return BigInteger.valueOf((Integer)val);
            else if (from == Long.class)
                return BigInteger.valueOf((Long)val);
        }
        else if (to == Float.class) {
            if (from == Boolean.class)
                return (float)(val == Boolean.TRUE ? 1 : 0);
            else if (from == Byte.class)
                return Float.valueOf((Byte)val);
            else if (from == Short.class)
                return Float.valueOf((Short)val);
            else if (from == Integer.class)
                return Float.valueOf((Integer)val);
            else if (from == Long.class)
                return Float.valueOf((Long)val);
            else if (BigInteger.class.isAssignableFrom(from))
                return ((Number)val).floatValue();
        }
        else if (to == Double.class) {
            if (from == Boolean.class)
                return (double)(val == Boolean.TRUE ? 1 : 0);
            else if (from == Byte.class)
                return Double.valueOf((Byte)val);
            else if (from == Short.class)
                return Double.valueOf((Short)val);
            else if (from == Integer.class)
                return Double.valueOf((Integer)val);
            else if (from == Long.class)
                return Double.valueOf((Long)val);
            else if (BigInteger.class.isAssignableFrom(from))
                return ((Number)val).doubleValue();
            else if (from == Float.class)
                return Double.valueOf((Float)val);
        }
        else if (BigDecimal.class.isAssignableFrom(to)) {
            if (from == Boolean.class)
                return val == Boolean.TRUE ? BigDecimal.ONE : BigDecimal.ZERO;
            else if (from == Byte.class)
                return BigDecimal.valueOf((Byte)val);
            else if (from == Short.class)
                return BigDecimal.valueOf((Short)val);
            else if (from == Integer.class)
                return BigDecimal.valueOf((Integer)val);
            else if (from == Long.class)
                return BigDecimal.valueOf((Long)val);
            else if (BigInteger.class.isAssignableFrom(from))
                return new BigDecimal((BigInteger)val);
            else if (from == Float.class)
                return BigDecimal.valueOf((Float)val);
            else if (from == Double.class)
                return BigDecimal.valueOf((Double)val);
        }

        throw new IgniteException("Can't convert value from type " + from.getName() + " to " + to.getName());
    }
}
