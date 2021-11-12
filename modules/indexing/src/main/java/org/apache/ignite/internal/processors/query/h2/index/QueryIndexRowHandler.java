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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/** Maps CacheDataRow to IndexRow using H2 columns references. */
public class QueryIndexRowHandler implements InlineIndexRowHandler {
    /** Cache descriptor. */
    private final GridH2RowDescriptor cacheDesc;

    /** H2 index columns. */
    private final List<IndexColumn> h2IdxColumns;

    /** List of key types for inlined index keys. */
    private final List<InlineIndexKeyType> keyTypes;

    /** List of index key definitions. */
    private final List<IndexKeyDefinition> keyDefs;

    /** Index key type settings. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** H2 Table. */
    private final GridH2Table h2Table;

    /** */
    public QueryIndexRowHandler(GridH2Table h2table, List<IndexColumn> h2IdxColumns,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs, List<InlineIndexKeyType> keyTypes, IndexKeyTypeSettings keyTypeSettings) {
        this.h2IdxColumns = h2IdxColumns;
        this.keyTypes = keyTypes;
        this.keyDefs = Collections.unmodifiableList(new ArrayList<>(keyDefs.values()));
        this.h2Table = h2table;
        cacheDesc = h2table.rowDescriptor();
        this.keyTypeSettings = keyTypeSettings;
    }

    /** {@inheritDoc} */
    @Override public IndexKey indexKey(int idx, CacheDataRow row) {
        Object o = getKey(idx, row);

        return IndexKeyFactory.wrap(
            o, keyDefs.get(idx).idxType(), cacheDesc.context().cacheObjectContext(), keyTypeSettings);
    }

    /** */
    public List<IndexColumn> getH2IdxColumns() {
        return h2IdxColumns;
    }

    /** {@inheritDoc} */
    @Override public List<InlineIndexKeyType> inlineIndexKeyTypes() {
        return keyTypes;
    }

    /** {@inheritDoc} */
    @Override public List<IndexKeyDefinition> indexKeyDefinitions() {
        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyTypeSettings indexKeyTypeSettings() {
        return keyTypeSettings;
    }

    /** */
    private Object getKey(int idx, CacheDataRow row) {
        int cacheIdx = h2IdxColumns.get(idx).column.getColumnId();

        if (cacheDesc.isKeyColumn(cacheIdx))
            return key(row);

        else if (cacheDesc.isValueColumn(cacheIdx))
            return value(row);

        // columnValue ignores default columns (_KEY, _VAL), so make this shift.
        return cacheDesc.columnValue(row.key(), row.value(), cacheIdx - QueryUtils.DEFAULT_COLUMNS_COUNT);
    }

    /** {@inheritDoc} */
    @Override public int partition(CacheDataRow row) {
        Object key = key(row);

        return cacheDesc.context().affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public Object cacheKey(CacheDataRow row) {
        return key(row);
    }

    /** {@inheritDoc} */
    @Override public Object cacheValue(CacheDataRow row) {
        return value(row);
    }

    /**
     * @return H2 table.
     */
    public GridH2Table getTable() {
        return h2Table;
    }

    /** @return Cache key for specified cache row. */
    public Object key(CacheDataRow row) {
        KeyCacheObject key = row.key();

        Object o = getBinaryObject(key);

        if (o != null)
            return o;

        CacheObjectContext coctx = cacheDesc.context().cacheObjectContext();

        return key.value(coctx, false);
    }

    /** */
    private Object value(CacheDataRow row) {
        CacheObject val = row.value();

        Object o = getBinaryObject(val);

        if (o != null)
            return o;

        CacheObjectContext coctx = cacheDesc.context().cacheObjectContext();

        return row.value().value(coctx, false);
    }

    /** */
    private Object getBinaryObject(CacheObject o) {
        if (o instanceof BinaryObjectImpl) {
            ((BinaryObjectImpl)o).detachAllowed(true);
            o = ((BinaryObjectImpl)o).detach();
            return o;
        }

        return null;
    }
}
