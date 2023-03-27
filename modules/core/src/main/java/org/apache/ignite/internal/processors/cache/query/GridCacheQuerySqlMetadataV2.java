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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Cache metadata with not null field.
 */
public class GridCacheQuerySqlMetadataV2 extends GridCacheQueryManager.CacheSqlMetadata {
    /** */
    private static final long serialVersionUID = 0L;

    /** Not null fields. */
    private Map<String, Set<String>> notNullFields;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQuerySqlMetadataV2() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param types Types.
     * @param keyClasses Key classes map.
     * @param valClasses Value classes map.
     * @param fields Fields maps.
     * @param indexes Indexes.
     * @param notNullFields Not null fields.
     */
    GridCacheQuerySqlMetadataV2(@Nullable String cacheName, Collection<String> types, Map<String, String> keyClasses,
        Map<String, String> valClasses, Map<String, Map<String, String>> fields,
        Map<String, Collection<GridCacheSqlIndexMetadata>> indexes, Map<String, Set<String>> notNullFields) {
        super(cacheName, types, keyClasses, valClasses, fields, indexes);

        this.notNullFields = notNullFields;
    }

    /**
     * @param metas Meta data instances from different nodes.
     */
    GridCacheQuerySqlMetadataV2(Iterable<GridCacheQueryManager.CacheSqlMetadata> metas) {
        super(metas);

        notNullFields = new HashMap<>();

        for (GridCacheQueryManager.CacheSqlMetadata meta : metas) {
            if (meta instanceof GridCacheQuerySqlMetadataV2) {
                GridCacheQuerySqlMetadataV2 metaV2 = (GridCacheQuerySqlMetadataV2)meta;

                notNullFields.putAll(metaV2.notNullFields);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<String> notNullFields(String type) {
        return notNullFields.get(type);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeMap(out, notNullFields);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        notNullFields = U.readHashMap(in);
    }
}
