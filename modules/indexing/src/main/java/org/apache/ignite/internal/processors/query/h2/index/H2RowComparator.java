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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.JavaObjectKey;
import org.apache.ignite.internal.cache.query.index.sorted.NullKey;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Provide H2 logic of keys comparation.
 */
public class H2RowComparator implements IndexRowComparator {
    /** Cache context. */
    private final CacheObjectContext coctx;

    /** Table. */
    private final GridH2Table table;

    /** Ignite H2 session. */
    private final SessionInterface ses;

    /** */
    private final boolean inlineObjHash;

    /** */
    public H2RowComparator(GridH2Table table, boolean inlineObjHash) {
        this.table = table;
        this.inlineObjHash = inlineObjHash;
        coctx = table.rowDescriptor().context().cacheObjectContext();
        ses = table.rowDescriptor().indexing().connections().jdbcConnection().getSession();
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, Object v, int curType) {
        if (curType == IndexKeyTypes.UNKNOWN)
            return CANT_BE_COMPARE;

        if (v == NullKey.INSTANCE)
            return 1;

        int objType = InlineIndexKeyTypeRegistry.get(v.getClass(), curType, !inlineObjHash).type();

        int highOrder = Value.getHigherOrder(curType, objType);

        // H2 supports comparison between different types after casting them to single type.
        if (highOrder != objType && highOrder == curType) {
            Value va = DataType.convertToValue(ses, v, highOrder);
            va = va.convertTo(highOrder);

            Object objHighOrder = va.getObject();

            InlineIndexKeyType highType = InlineIndexKeyTypeRegistry.get(objHighOrder.getClass(), highOrder, !inlineObjHash);

            // The only way to invoke inline comparation again.
            return ((NullableInlineIndexKeyType) highType).compare0(pageAddr, off, objHighOrder);
        }

        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException {
        Object robject = right.getKey(idx);
        Object lobject = left.getKey(idx);

        if (lobject == NullKey.INSTANCE)
            return ((NullKey) lobject).compareTo(robject);
        else if (robject == NullKey.INSTANCE)
            return 1;
        else if (lobject == null)
            return CANT_BE_COMPARE;

        int ltype, rtype;

        // Side of comparison can be set by user in query with type that different for specified schema.
        if (left instanceof IndexSearchRowImpl)
            ltype = DataType.getTypeFromClass(lobject.getClass());
        else
            ltype = left.getRowHandler().getIndexKeyDefinitions().get(idx).getIdxType();

        if (right instanceof IndexSearchRowImpl)
            rtype = DataType.getTypeFromClass(robject.getClass());
        else
            rtype = right.getRowHandler().getIndexKeyDefinitions().get(idx).getIdxType();

        int c = compareValues(wrap(lobject, ltype), wrap(robject, rtype));

        return Integer.signum(c);
    }

    /** */
    private Value wrap(Object val, int type) throws IgniteCheckedException {
        Object o = val;

        if (val instanceof JavaObjectKey)
            o = ((JavaObjectKey) val).getKey();

        return H2Utils.wrap(coctx, o, type);
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public int compareValues(Value v1, Value v2) throws IgniteCheckedException {
        try {
            return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);

        } catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }
}
