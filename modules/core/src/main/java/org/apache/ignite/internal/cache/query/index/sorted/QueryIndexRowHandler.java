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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;

/** Maps CacheDataRow to IndexRow using columns references. */
public class QueryIndexRowHandler implements InlineIndexRowHandler {
    /** Cache info. */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** List of key types for inlined index keys. */
    private final List<InlineIndexKeyType> keyTypes;

    /** List of index key definitions. */
    private final List<IndexKeyDefinition> keyDefs;

    /** Index key type settings. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** Query properties for each index key. */
    private final GridQueryProperty[] props;

    /** */
    public QueryIndexRowHandler(
        GridQueryTypeDescriptor type,
        GridCacheContextInfo<?, ?> cacheInfo,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        List<InlineIndexKeyType> keyTypes,
        IndexKeyTypeSettings keyTypeSettings
    ) {
        this.keyTypes = Collections.unmodifiableList(keyTypes);
        this.keyDefs = Collections.unmodifiableList(new ArrayList<>(keyDefs.values()));

        props = new GridQueryProperty[keyDefs.size()];
        int propIdx = 0;

        for (String propName : keyDefs.keySet()) {
            GridQueryProperty prop;

            if (propName.equals(QueryUtils.KEY_FIELD_NAME) || propName.equals(type.keyFieldName())
                || propName.equals(type.keyFieldAlias()))
                prop = new KeyOrValPropertyWrapper(true, propName, type.keyClass());
            else if (propName.equals(QueryUtils.VAL_FIELD_NAME) || propName.equals(type.valueFieldName())
                || propName.equals(type.valueFieldAlias()))
                prop = new KeyOrValPropertyWrapper(false, propName, type.valueClass());
            else
                prop = type.property(propName);

            assert prop != null : propName;

            props[propIdx++] = prop;
        }

        this.cacheInfo = cacheInfo;
        this.keyTypeSettings = keyTypeSettings;
    }

    /** {@inheritDoc} */
    @Override public IndexKey indexKey(int idx, CacheDataRow row) {
        try {
            return IndexKeyFactory.wrap(
                props[idx].value(row.key(), row.value()),
                keyDefs.get(idx).idxType(),
                cacheInfo.cacheContext().cacheObjectContext(),
                keyTypeSettings
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
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

    /** {@inheritDoc} */
    @Override public int partition(CacheDataRow row) {
        Object key = unwrap(row.key());

        return cacheInfo.cacheContext().affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public Object cacheKey(CacheDataRow row) {
        return unwrap(row.key());
    }

    /** {@inheritDoc} */
    @Override public Object cacheValue(CacheDataRow row) {
        return unwrap(row.value());
    }

    /** */
    private Object unwrap(CacheObject val) {
        Object o = getBinaryObject(val);

        if (o != null)
            return o;

        CacheObjectContext coctx = cacheInfo.cacheContext().cacheObjectContext();

        return val.value(coctx, false);
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

    /** */
    private class KeyOrValPropertyWrapper extends QueryUtils.KeyOrValProperty {
        /** */
        public KeyOrValPropertyWrapper(boolean key, String name, Class<?> cls) {
            super(key, name, cls);
        }

        /** {@inheritDoc} */
        @Override public Object value(Object key, Object val) {
            return key() ? unwrap((CacheObject)key) : unwrap((CacheObject)val);
        }
    }
}
