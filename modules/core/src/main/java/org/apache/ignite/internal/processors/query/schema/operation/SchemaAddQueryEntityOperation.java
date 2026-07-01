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

package org.apache.ignite.internal.processors.query.schema.operation;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.schema.message.QueryEntityExMessage;
import org.apache.ignite.internal.processors.query.schema.message.QueryEntityMessage;
import org.apache.ignite.internal.util.typedef.F;

/** Operation, which enables indexing on cache operation. */
public class SchemaAddQueryEntityOperation extends SchemaAbstractOperation {
    /** Query entities messages. */
    @Order(0)
    Collection<QueryEntityMessage> entitiesMsgs;

    /** Original query entities. We keep them to avoid unneccessary conversions from messages. */
    private Collection<QueryEntity> entities;

    /** */
    @Order(1)
    int qryParallelism;

    /** */
    @Order(2)
    boolean sqlEscape;

    /** */
    public SchemaAddQueryEntityOperation() {}

    /**
     * @param opId Operation ID.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param entities Collection of QueryEntity.
     * @param qryParallelism Query parallelism.
     * @param sqlEscape Sql escape flag.
     */
    public SchemaAddQueryEntityOperation(
        UUID opId,
        String cacheName,
        String schemaName,
        Collection<QueryEntity> entities,
        int qryParallelism,
        boolean sqlEscape
    ) {
        super(opId, cacheName, schemaName);
        this.entities = entities;
        this.qryParallelism = qryParallelism;
        this.sqlEscape = sqlEscape;

        entitiesMsgs = F.viewReadOnly(entities, this::makeEntityMessage);
    }

    /** @return Collection of query entities. */
    public Collection<QueryEntity> entities() {
        if (entities == null)
            entities = F.viewReadOnly(entitiesMsgs, QueryEntityMessage::toEntity);

        return entities;
    }

    /** @return Query parallelism. */
    public int queryParallelism() {
        return qryParallelism;
    }

    /** @return Sql escape flag. */
    public boolean isSqlEscape() {
        return sqlEscape;
    }

    /**
     * @param qryEntity Query entity.
     * @return The appropriate query entity message.
     */
    private QueryEntityMessage makeEntityMessage(QueryEntity qryEntity) {
        return qryEntity instanceof QueryEntityEx ? new QueryEntityExMessage((QueryEntityEx)qryEntity)
            : new QueryEntityMessage(qryEntity);
    }
}
