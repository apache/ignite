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

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.QueryEntityEx;

/** Message for {@link QueryEntityEx} transfer. */
public class QueryEntityExMessage extends QueryEntityMessage {
    /** Whether to preserve order specified by 'keyFields' or not. */
    @Order(0)
    boolean preserveKeysOrder;

    /** Whether a primary key should be autocreated or not. */
    @Order(1)
    boolean implicitPk;

    /** Whether absent PK parts should be filled with defaults or not. */
    @Order(2)
    boolean fillAbsentPKsWithDefaults;

    /** INLINE_SIZE for PK index. */
    @Order(3)
    int pkInlineSize;

    /** INLINE_SIZE for affinity field index. */
    @Order(4)
    int affKeyInlineSize;

    /** Whether query entity was created by SQL. */
    @Order(5)
    boolean sql;

    /** */
    public QueryEntityExMessage() { }

    /**
     * @param qryEntity Original {@link QueryEntity}.
     */
    public QueryEntityExMessage(QueryEntityEx qryEntity) {
        super(qryEntity);

        preserveKeysOrder = qryEntity.isPreserveKeysOrder();
        implicitPk = qryEntity.implicitPk();
        fillAbsentPKsWithDefaults = qryEntity.fillAbsentPKsWithDefaults();

        pkInlineSize = qryEntity.getPrimaryKeyInlineSize() != null ? qryEntity.getPrimaryKeyInlineSize() : -1;

        affKeyInlineSize = qryEntity.getAffinityKeyInlineSize() != null ? qryEntity.getAffinityKeyInlineSize() : -1;

        sql = qryEntity.sql();
    }

    /** {@inheritDoc} */
    @Override public QueryEntity toEntity() {
        QueryEntity baseEntity = super.toEntity();

        // We should nullify '_notNullFields' field of base entity,
        // because 'QueryEntityEx' uses extra 'notNullFields' field for storing of not null fields.
        baseEntity.setNotNullFields(null);

        QueryEntityEx qryEntity = new QueryEntityEx(baseEntity);

        qryEntity.setNotNullFields(notNullFields);
        qryEntity.setPreserveKeysOrder(preserveKeysOrder);
        qryEntity.implicitPk(implicitPk);
        qryEntity.fillAbsentPKsWithDefaults(fillAbsentPKsWithDefaults);
        qryEntity.sql(sql);

        if (pkInlineSize != -1)
            qryEntity.setPrimaryKeyInlineSize(pkInlineSize);

        if (affKeyInlineSize != -1)
            qryEntity.setAffinityKeyInlineSize(affKeyInlineSize);

        return qryEntity;
    }
}
