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

package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.QueryIndexMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.ObjectData;

/** Message for {@link QueryEntity}. */
public class QueryEntityMessage implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

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

    /** Fields available for query. A map from field name to type name. */
    @Order(4)
    @GridToStringInclude
    LinkedHashMap<String, String> fields;

    /** Set of field names that belong to the key. */
    @Order(5)
    @GridToStringInclude
    Set<String> keyFields;

    /** Aliases. */
    @Order(6)
    @GridToStringInclude
    Map<String, String> aliases;

    /** Collection of query indexes. */
    @Order(7)
    @GridToStringInclude
    Collection<QueryIndexMessage> idxs;

    /** Table name. */
    @Order(8)
    String tableName;

    /** Fields that must have non-null value. NB: DO NOT remove underscore to avoid clashes with QueryEntityEx. */
    @Order(9)
    Set<String> notNullFields;

    /** Fields default values. */
    @Order(10)
    Map<String, ObjectData> dfltFieldValues;

    /** Precision (maximum length) for fields. */
    @Order(11)
    Map<String, Integer> fieldsPrecision;

    /** Scale for fields. */
    @Order(12)
    Map<String, Integer> fieldsScale;

    /** Empty constructor for {@link MessageFactory}. */
    public QueryEntityMessage() {
        // No-op.
    }

    /** Copies {@code qryEntity}. */
    public QueryEntityMessage(QueryEntity qryEntity) {
        assert qryEntity != null;

        keyType = qryEntity.getKeyType();
        valType = qryEntity.getValueType();
        keyFieldName = qryEntity.getKeyFieldName();
        valFieldName = qryEntity.getValueFieldName();
        fields = qryEntity.getFields();
        keyFields = qryEntity.getKeyFields();
        aliases = qryEntity.getAliases();
        idxs = F.viewReadOnly(qryEntity.getIndexes(), QueryIndexMessage::new);
        tableName = qryEntity.getTableName();
        notNullFields = qryEntity.getNotNullFields();
        dfltFieldValues = F.viewReadOnly(qryEntity.getDefaultFieldValues(), val -> new ObjectData((Serializable)val));
        fieldsPrecision = qryEntity.getFieldsPrecision();
        fieldsScale = qryEntity.getFieldsScale();
    }

    /** @return Copy of {@code msg} as {@link QueryEntity}. */
    public static QueryEntity queryEntity(QueryEntityMessage msg) {
        assert msg != null;

        QueryEntity res = new QueryEntity();

        res.setKeyType(msg.keyType);
        res.setValueType(msg.valType);
        res.setKeyFieldName(msg.keyFieldName);
        res.setValueFieldName(msg.valFieldName);
        res.setFields(msg.fields);
        res.setKeyFields(msg.keyFields);
        res.setAliases(msg.aliases);
        res.setIndexes(F.viewReadOnly(msg.idxs, QueryIndexMessage::queryIndex));
        res.setTableName(msg.tableName);
        res.setNotNullFields(msg.notNullFields);
        res.setDefaultFieldValues(F.viewReadOnly(msg.dfltFieldValues, ObjectData::unwrap));
        res.setFieldsPrecision(msg.fieldsPrecision);
        res.setFieldsScale(msg.fieldsScale);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityMessage.class, this);
    }
}
