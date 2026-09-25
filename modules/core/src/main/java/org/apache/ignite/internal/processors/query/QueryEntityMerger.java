/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.typedef.F;

/** Utility for merging compatible {@link QueryEntity} metadata. */
public final class QueryEntityMerger {
    /** */
    public static final String CONFLICT_MESSAGE_TEMPLATE = "Failed to merge query entities due to conflicting metadata " +
        "[cacheName=%s, property=%s, existingValue=%s, incomingValue=%s]";

    /** */
    private final String cacheName;

    /** */
    private QueryEntityMerger(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Merges incoming query entity metadata into existing entity.
     *
     * @param cacheName Cache name.
     * @param existing Existing query entity.
     * @param incoming Incoming query entity.
     * @return Merged query entity.
     * @throws CacheException If entities contain conflicting metadata.
     */
    public static QueryEntity merge(String cacheName, QueryEntity existing, QueryEntity incoming) {
        return new QueryEntityMerger(cacheName).merge0(existing, incoming);
    }

    /** */
    private QueryEntity merge0(QueryEntity ex, QueryEntity in) {
        if (!Objects.equals(ex.findValueType(), in.findValueType())) {
            throw new CacheException(
                "Failed to merge query entities because value types differ " +
                    "[cacheName=" + cacheName +
                    ", existingValueType=" + ex.findValueType() +
                    ", incomingValueType=" + in.findValueType() + ']'
            );
        }

        QueryEntity res = new QueryEntity(ex);

        res.setKeyType(mergeKeyType(ex, in));

        res.setValueType(mergeProperty("valueType", ex.getValueType(), in.getValueType()));
        res.setTableName(mergeProperty("tableName", ex.getTableName(), in.getTableName()));
        res.setKeyFieldName(mergeProperty("keyFieldName", ex.getKeyFieldName(), in.getKeyFieldName()));
        res.setValueFieldName(mergeProperty("valueFieldName", ex.getValueFieldName(), in.getValueFieldName()));

        res.setFields(mergeFields(ex.getFields(), in.getFields()));

        res.setKeyFields(mergeSet(ex.getKeyFields(), in.getKeyFields()));
        res.setNotNullFields(mergeSet(ex.getNotNullFields(), in.getNotNullFields()));

        res.setAliases(mergeMap("aliases", ex.getAliases(), in.getAliases()));
        res.setDefaultFieldValues(mergeMap("defaultFieldValues", ex.getDefaultFieldValues(), in.getDefaultFieldValues()));
        res.setFieldsPrecision(mergeMap("fieldsPrecision", ex.getFieldsPrecision(), in.getFieldsPrecision()));
        res.setFieldsScale(mergeMap("fieldsScale", ex.getFieldsScale(), in.getFieldsScale()));

        res.setIndexes(mergeIndexes(res, ex.getIndexes(), in.getIndexes()));

        return res;
    }

    /** */
    private String mergeKeyType(QueryEntity ex, QueryEntity in) {
        String exKeyType = ex.findKeyType();
        String inKeyType = in.findKeyType();

        if (exKeyType != null && inKeyType != null && !Objects.equals(exKeyType, inKeyType)) {
            throw mergeConflict(
                "keyType",
                exKeyType,
                inKeyType
            );
        }

        return ex.getKeyType() != null ? ex.getKeyType() : in.getKeyType();
    }

    /** */
    private <T> T mergeProperty(String propName, T existingVal, T incomingVal) {
        if (existingVal == null)
            return incomingVal;

        if (incomingVal == null)
            return existingVal;

        if (Objects.equals(existingVal, incomingVal))
            return existingVal;

        throw mergeConflict(propName, existingVal, incomingVal);
    }

    /** */
    private LinkedHashMap<String, String> mergeFields(
        Map<String, String> existingFields,
        Map<String, String> incomingFields
    ) {
        if (existingFields == null && incomingFields == null)
            return null;

        LinkedHashMap<String, String> res = new LinkedHashMap<>();

        if (existingFields != null)
            res.putAll(existingFields);

        if (incomingFields == null)
            return res;

        for (Map.Entry<String, String> entry : incomingFields.entrySet()) {
            String field = entry.getKey();
            String incomingType = entry.getValue();

            if (!res.containsKey(field)) {
                res.put(field, incomingType);

                continue;
            }

            String existingType = res.get(field);

            if (!Objects.equals(existingType, incomingType))
                throw mergeConflict("fieldType[" + field + ']', existingType, incomingType);
        }

        return res;
    }

    /** */
    private <T> Map<String, T> mergeMap(String propName, Map<String, T> existingVals, Map<String, T> incomingVals) {
        if (existingVals == null && incomingVals == null)
            return null;

        Map<String, T> res = new HashMap<>();

        if (existingVals != null)
            res.putAll(existingVals);

        if (incomingVals == null)
            return res;

        for (Map.Entry<String, T> entry : incomingVals.entrySet()) {
            String field = entry.getKey();
            T incomingVal = entry.getValue();

            if (!res.containsKey(field)) {
                res.put(field, incomingVal);

                continue;
            }

            T existingVal = res.get(field);

            if (!Objects.equals(existingVal, incomingVal))
                throw mergeConflict(propName + '[' + field + ']', existingVal, incomingVal);
        }

        return res;
    }

    /** */
    private <T> Set<T> mergeSet(Set<T> existing, Set<T> incoming) {
        if (F.isEmpty(existing) && F.isEmpty(incoming))
            return null;

        Set<T> res = new LinkedHashSet<>();

        if (existing != null)
            res.addAll(existing);

        if (incoming != null)
            res.addAll(incoming);

        return res;
    }

    /** */
    private Collection<QueryIndex> mergeIndexes(
        QueryEntity entity,
        Collection<QueryIndex> existingIndexes,
        Collection<QueryIndex> incomingIndexes
    ) {
        if (F.isEmpty(existingIndexes) && F.isEmpty(incomingIndexes))
            return null;

        List<QueryIndex> res = new ArrayList<>();

        Map<String, QueryIndex> indexesByName = new HashMap<>();

        if (existingIndexes != null) {
            for (QueryIndex idx : existingIndexes) {
                String idxName = QueryUtils.indexName(entity, idx);

                res.add(idx);

                indexesByName.put(idxName, idx);
            }
        }

        if (incomingIndexes == null)
            return res;

        for (QueryIndex incomingIdx : incomingIndexes) {
            String idxName = QueryUtils.indexName(entity, incomingIdx);

            QueryIndex existingIdx = indexesByName.get(idxName);

            if (existingIdx == null) {
                res.add(incomingIdx);

                indexesByName.put(idxName, incomingIdx);

                continue;
            }

            if (!existingIdx.equals(incomingIdx))
                throw mergeConflict("index[" + idxName + ']', existingIdx, incomingIdx);
        }

        return res;
    }

    /** */
    private CacheException mergeConflict(String propName, Object existingVal, Object incomingVal) {
        return new CacheException(
            String.format(CONFLICT_MESSAGE_TEMPLATE, cacheName, propName, existingVal, incomingVal)
        );
    }
}
