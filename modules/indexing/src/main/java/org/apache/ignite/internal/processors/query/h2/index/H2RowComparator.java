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
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCompartorImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
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
        ses = table.rowDescriptor().indexing().connections().jdbcConnection().getSession();
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, int curType) {
        int cmp = super.compareKey(pageAddr, off, maxSize, key, curType);

        if (cmp != COMPARE_UNSUPPORTED)
            return cmp;

        int objType = InlineIndexKeyTypeRegistry.get(key, curType, keyTypeSettings).type();

        int highOrder = Value.getHigherOrder(curType, objType);

        // H2 supports comparison between different types after casting them to single type.
        if (highOrder != objType && highOrder == curType) {
            Value va = DataType.convertToValue(ses, key.key(), highOrder);
            va = va.convertTo(highOrder);

            IndexKey objHighOrder = IndexKeyFactory.wrap(
                va.getObject(), highOrder, coctx, keyTypeSettings);

            InlineIndexKeyType highType = InlineIndexKeyTypeRegistry.get(objHighOrder, highOrder, keyTypeSettings);

            // The only way to invoke inline comparation again.
            return ((NullableInlineIndexKeyType) highType).compare0(pageAddr, off, objHighOrder);
        }

        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException {
        int cmp = super.compareKey(left, right, idx);

        if (cmp != COMPARE_UNSUPPORTED)
            return cmp;

        int ltype, rtype;

        Object lobject = left.key(idx).key();
        Object robject = right.key(idx).key();

        // Side of comparison can be set by user in query with type that different for specified schema.
        if (left.indexSearchRow())
            ltype = DataType.getTypeFromClass(lobject.getClass());
        else
            ltype = left.rowHandler().indexKeyDefinitions().get(idx).idxType();

        if (right.indexSearchRow())
            rtype = DataType.getTypeFromClass(robject.getClass());
        else
            rtype = right.rowHandler().indexKeyDefinitions().get(idx).idxType();

        int c = compareValues(wrap(lobject, ltype), wrap(robject, rtype));

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
            return v1 == v2 ? 0 : table.compareTypeSafe(v1, v2);

        } catch (DbException ex) {
            throw new IgniteCheckedException("Rows cannot be compared", ex);
        }
    }
}
