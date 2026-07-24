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

package org.apache.ignite.internal.processors.query.schema.message;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.QueryIndexMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Message for {@link QueryEntity} transfer.
 */
public class QueryEntityMessage implements MarshallableMessage {
    /** Key type. */
    @Order(0)
    String keyType;

    /** Value type. */
    @Order(1)
    String valType;

    /** Key name. */
    @Order(2)
    String keyFieldName;

    /** Value name. */
    @Order(3)
    String valFieldName;

    /** Fields available for query. */
    @Order(4)
    LinkedHashMap<String, String> fields;

    /** Set of field names that belong to the key. */
    @Order(5)
    String[] keyFields;

    /** Aliases. */
    @Order(6)
    Map<String, String> aliases;

    /** Collection of query indexes. */
    @Order(7)
    Collection<QueryIndexMessage> idxs;

    /** Table name. */
    @Order(8)
    String tableName;

    /**
     * Fields that must have non-null value.
     * NB: Used in serde of both QueryEntity and QueryEntityEx in order to avoid duplication.
     */
    @Order(9)
    Set<String> notNullFields;

    /** Fields default values. */
    Map<String, Object> dfltFieldValues;

    /** Serialized form of {@link #dfltFieldValues}. */
    @Order(10)
    byte[] dfltFieldValuesBytes;

    /** Precision(Maximum length) for fields. */
    @Order(11)
    Map<String, Integer> fieldsPrecision;

    /** Scale for fields. */
    @Order(12)
    Map<String, Integer> fieldsScale;

    /** */
    public QueryEntityMessage() { }

    /** @param qryEntity Original {@link QueryEntity}. */
    public QueryEntityMessage(QueryEntity qryEntity) {
        keyType = qryEntity.getKeyType();
        valType = qryEntity.getValueType();

        keyFieldName = qryEntity.getKeyFieldName();
        valFieldName = qryEntity.getValueFieldName();

        fields = qryEntity.getFields();
        aliases = qryEntity.getAliases();

        if (!F.isEmpty(qryEntity.getKeyFields()))
            keyFields = qryEntity.getKeyFields().toArray(U.EMPTY_STRS);

        if (!F.isEmpty(qryEntity.getIndexes()))
            idxs = F.viewReadOnly(qryEntity.getIndexes(), QueryIndexMessage::new);

        tableName = qryEntity.getTableName();

        notNullFields = qryEntity.getNotNullFields();
        dfltFieldValues = qryEntity.getDefaultFieldValues();
        fieldsPrecision = qryEntity.getFieldsPrecision();
        fieldsScale = qryEntity.getFieldsScale();
    }

    /** @return Original {@link QueryEntity}. */
    public QueryEntity toEntity() {
        return entityBase().setNotNullFields(notNullFields);
    }

    /** @return Entity without 'notNullFields.' */
    protected QueryEntity entityBase() {
        return new QueryEntity()
            .setKeyType(keyType)
            .setValueType(valType)
            .setKeyFieldName(keyFieldName)
            .setValueFieldName(valFieldName)
            .setFields(fields)
            .setKeyFields(!F.isEmpty(keyFields) ? new LinkedHashSet<>(List.of(keyFields)) : null)
            .setAliases(aliases)
            .setIndexes(!F.isEmpty(idxs) ? F.transform(idxs, QueryIndexMessage::queryIndex) : null)
            .setTableName(tableName)
            .setDefaultFieldValues(dfltFieldValues)
            .setFieldsPrecision(fieldsPrecision)
            .setFieldsScale(fieldsScale);
    }

    /** {@inheritDoc} */
    @Override public void marshal(Marshaller marsh) throws IgniteCheckedException {
        if (!F.isEmpty(dfltFieldValues) && dfltFieldValuesBytes == null)
            dfltFieldValuesBytes = U.marshal(marsh, dfltFieldValues);
    }

    /** {@inheritDoc} */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (!F.isEmpty(dfltFieldValuesBytes)) {
            dfltFieldValues = U.unmarshal(marsh, dfltFieldValuesBytes, clsLdr);

            dfltFieldValuesBytes = null;
        }
    }
}
