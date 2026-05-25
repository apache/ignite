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

package org.apache.ignite.internal.management.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.QueryIndexMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/**
 * Message for {@link org.apache.ignite.cache.QueryEntity}.
 */
public class QueryEntity implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key class used to store key in cache. */
    @Order(0)
    String keyType;

    /** Value class used to store value in cache. */
    @Order(1)
    String valType;

    /** Fields to be queried, in addition to indexed fields. */
    @Order(2)
    Map<String, String> qryFlds;

    /** Key fields. */
    @Order(3)
    List<String> keyFields;

    /** Aliases. */
    @Order(4)
    Map<String, String> aliases;

    /** Table name. */
    @Order(5)
    String tblName;

    /** Key name. Can be used in field list to denote the key as a whole. */
    @Order(6)
    String keyFieldName;

    /** Value name. Can be used in field list to denote the entire value. */
    @Order(7)
    String valFieldName;

    /** Fields to create group indexes for. */
    @Order(8)
    List<QueryIndexMessage> grps;

    /**
     * @param qryEntities Collection of query entities.
     * @return Messages for query entities.
     */
    public static List<QueryEntity> list(Collection<org.apache.ignite.cache.QueryEntity> qryEntities) {
        List<QueryEntity> entities = new ArrayList<>();

        // Add query entries.
        if (!F.isEmpty(qryEntities))
            for (org.apache.ignite.cache.QueryEntity qryEntity : qryEntities)
                entities.add(new QueryEntity(qryEntity));

        return entities;
    }

    /**
     * @param qryEntities Collection of messages for query entities.
     * @return Query entities from messages.
     */
    public static Collection<org.apache.ignite.cache.QueryEntity> unwrapList(Collection<QueryEntity> qryEntities) {
        Collection<org.apache.ignite.cache.QueryEntity> entities = new ArrayList<>();

        if (!F.isEmpty(qryEntities)) {
            for (QueryEntity entityDto : qryEntities) {
                org.apache.ignite.cache.QueryEntity entity = new org.apache.ignite.cache.QueryEntity();

                entity.setKeyType(entityDto.keyType);
                entity.setValueType(entityDto.valType);
                entity.setFields((LinkedHashMap<String, String>)entityDto.qryFlds);
                entity.setKeyFields(entityDto.keyFields != null ? new LinkedHashSet<>(entityDto.keyFields) : null);
                entity.setAliases(entityDto.aliases);
                entity.setTableName(entityDto.tblName);
                entity.setKeyFieldName(entityDto.keyFieldName);
                entity.setValueFieldName(entityDto.valFieldName);
                entity.setIndexes(F.viewReadOnly(entityDto.grps, QueryIndexMessage::queryIndex));

                entities.add(entity);
            }
        }

        return entities;
    }

    /** Empty constructor for a {@link MessageFactory}. */
    public QueryEntity() {
        // No-op.
    }

    /**
     * Create message for given cache type metadata.
     *
     * @param q Actual cache query entities.
     */
    private QueryEntity(org.apache.ignite.cache.QueryEntity q) {
        assert q != null;

        keyType = q.getKeyType();
        valType = q.getValueType();

        keyFields = q.getKeyFields() != null ? new ArrayList<>(q.getKeyFields()) : null;

        LinkedHashMap<String, String> qryFields = q.getFields();

        qryFlds = new LinkedHashMap<>(qryFields);

        aliases = new HashMap<>(q.getAliases());

        Collection<org.apache.ignite.cache.QueryIndex> qryIdxs = q.getIndexes();

        grps = new ArrayList<>(qryIdxs.size());

        for (org.apache.ignite.cache.QueryIndex qryIdx : qryIdxs)
            grps.add(new QueryIndexMessage(qryIdx));

        tblName = q.getTableName();
        keyFieldName = q.getKeyFieldName();
        valFieldName = q.getValueFieldName();
    }

    /**
     * @return Key class used to store key in cache.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * @return Value class used to store value in cache.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * @return Key fields.
     */
    public List<String> getKeyFields() {
        return keyFields;
    }

    /**
     * @return Fields to be queried, in addition to indexed fields.
     */
    public Map<String, String> getQueryFields() {
        return qryFlds;
    }

    /**
     * @return Field aliases.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * @return Key name. Can be used in field list to denote the key as a whole.
     */
    public String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * @return Value name. Can be used in field list to denote the entire value.
     */
    public String getValueFieldName() {
        return valFieldName;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public List<QueryIndexMessage> getGroups() {
        return grps;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntity.class, this);
    }
}
