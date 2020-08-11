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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Metadata job.
 */
@GridInternal
class GridCacheQuerySqlMetadataJobV2 implements IgniteCallable<Collection<GridCacheQueryManager.CacheSqlMetadata>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Number of fields to report when no fields defined. Includes _key and _val columns. */
    private static final int NO_FIELDS_COLUMNS_COUNT = 2;

    /** Grid */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Collection<GridCacheQueryManager.CacheSqlMetadata> call() {
        final GridKernalContext ctx = ((IgniteKernal)ignite).context();

        Collection<String> cacheNames = F.viewReadOnly(ctx.cache().caches(),
            new C1<IgniteInternalCache<?, ?>, String>() {
                @Override public String apply(IgniteInternalCache<?, ?> c) {
                    return c.name();
                }
            },
            new P1<IgniteInternalCache<?, ?>>() {
                @Override public boolean apply(IgniteInternalCache<?, ?> c) {
                    return !CU.isSystemCache(c.name()) && !DataStructuresProcessor.isDataStructureCache(c.name());
                }
            }
        );

        return F.transform(cacheNames, new C1<String, GridCacheQueryManager.CacheSqlMetadata>() {
            @Override public GridCacheQueryManager.CacheSqlMetadata apply(String cacheName) {
                Collection<GridQueryTypeDescriptor> types = ctx.query().types(cacheName);

                Collection<String> names = U.newHashSet(types.size());
                Map<String, String> keyClasses = U.newHashMap(types.size());
                Map<String, String> valClasses = U.newHashMap(types.size());
                Map<String, Map<String, String>> fields = U.newHashMap(types.size());
                Map<String, Collection<GridCacheSqlIndexMetadata>> indexes = U.newHashMap(types.size());
                Map<String, Set<String>> notNullFields = U.newHashMap(types.size());

                for (GridQueryTypeDescriptor type : types) {
                    // Filter internal types (e.g., data structures).
                    if (type.name().startsWith("GridCache"))
                        continue;

                    names.add(type.name());

                    keyClasses.put(type.name(), type.keyClass().getName());
                    valClasses.put(type.name(), type.valueClass().getName());

                    int size = type.fields().isEmpty() ? NO_FIELDS_COLUMNS_COUNT : type.fields().size();

                    Map<String, String> fieldsMap = U.newLinkedHashMap(size);
                    HashSet<String> notNullFieldsSet = U.newHashSet(1);

                    // _KEY and _VAL are not included in GridIndexingTypeDescriptor.valueFields
                    if (type.fields().isEmpty()) {
                        fieldsMap.put("_KEY", type.keyClass().getName());
                        fieldsMap.put("_VAL", type.valueClass().getName());
                    }

                    for (Map.Entry<String, Class<?>> e : type.fields().entrySet()) {
                        String fieldName = e.getKey();

                        fieldsMap.put(fieldName.toUpperCase(), e.getValue().getName());

                        if (type.property(fieldName).notNull())
                            notNullFieldsSet.add(fieldName.toUpperCase());
                    }

                    fields.put(type.name(), fieldsMap);
                    notNullFields.put(type.name(), notNullFieldsSet);

                    Map<String, GridQueryIndexDescriptor> idxs = type.indexes();

                    Collection<GridCacheSqlIndexMetadata> indexesCol = new ArrayList<>(idxs.size());

                    for (Map.Entry<String, GridQueryIndexDescriptor> e : idxs.entrySet()) {
                        GridQueryIndexDescriptor desc = e.getValue();

                        // Add only SQL indexes.
                        if (desc.type() == QueryIndexType.SORTED) {
                            Collection<String> idxFields = new LinkedList<>();
                            Collection<String> descendings = new LinkedList<>();

                            for (String idxField : e.getValue().fields()) {
                                String idxFieldUpper = idxField.toUpperCase();

                                idxFields.add(idxFieldUpper);

                                if (desc.descending(idxField))
                                    descendings.add(idxFieldUpper);
                            }

                            indexesCol.add(new GridCacheQueryManager.CacheSqlIndexMetadata(e.getKey().toUpperCase(),
                                idxFields, descendings, false));
                        }
                    }

                    indexes.put(type.name(), indexesCol);
                }

                return new GridCacheQuerySqlMetadataV2(cacheName, names, keyClasses, valClasses, fields, indexes,
                    notNullFields);
            }
        });
    }
}
