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

import java.util.UUID;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema create table operation.
 */
public class SchemaCreateTableOperation extends SchemaAbstractOperation {
    /** Query entity to create table from. */
    @GridToStringInclude
    private final QueryEntity entity;

    /** Cache name to take settings from. */
    private final String tplCacheName;

    /** Quietly ignore this command if table already exists. */
    private final boolean ifNotExists;

    /**
     * Constructor.
     * @param opId  Operation ID.
     * @param space Space.
     * @param entity Query entity to create table from.
     * @param tplCacheName Cache name to take settings from.
     * @param ifNotExists Quietly ignore this command if table already exists.
     */
    public SchemaCreateTableOperation(UUID opId, String space, QueryEntity entity, String tplCacheName,
        boolean ifNotExists) {
        super(opId, space);
        this.entity = entity;
        this.tplCacheName = tplCacheName;
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Query entity to create table from.
     */
    public QueryEntity entity() {
        return entity;
    }

    /**
     * @return tplCacheName Cache name to take settings from.
     */
    public String templateCacheName() {
        return tplCacheName;
    }

    /**
     * @return Quietly ignore this command if table already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaCreateTableOperation.class, this, "parent", super.toString());
    }
}
