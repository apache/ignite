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

package org.apache.ignite.internal.processors.query.h2.ddl;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import java.util.UUID;

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class DdlCreateIndexOperation extends DdlAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index params. */
    @GridToStringInclude
    private final QueryIndex idx;

    /** Cache name. */
    private final String schemaName;

    /** Table name. */
    private final String tblName;

    /** Ignore operation if index exists. */
    private final boolean ifNotExists;

    /**
     * Constructor.
     *
     * @param opId Operation id.
     * @param cliNodeId Id of node that initiated this operation.
     * @param idx Index params.
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param ifNotExists Ignore operation if index exists.
     */
    DdlCreateIndexOperation(IgniteUuid opId, UUID cliNodeId, QueryIndex idx, String schemaName, String tblName,
        boolean ifNotExists) {
        super(opId, cliNodeId);

        this.idx = idx;
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Index params.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Ignore operation if index exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DdlCreateIndexOperation.class, this);
    }
}
