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

package org.apache.ignite.internal.processors.query.ddl;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class DdlCreateIndexOperation extends DdlAbstractIndexOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Space. */
    private final String space;

    /** Table name. */
    private final String tblName;

    /** Index. */
    @GridToStringInclude
    private final QueryIndex idx;

    /** Ignore operation if index exists. */
    private final boolean ifNotExists;

    /**
     * Constructor.
     *
     * @param cliNodeId Id of node that initiated this operation.
     * @param opId Operation id.
     * @param space Space.
     * @param tblName Table name.
     * @param idx Index params.
     * @param ifNotExists Ignore operation if index exists.
     */
    DdlCreateIndexOperation(UUID cliNodeId, UUID opId, String space, String tblName, QueryIndex idx,
        boolean ifNotExists) {
        super(cliNodeId, opId);

        this.space = space;
        this.tblName = tblName;
        this.idx = idx;
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
    public String space() {
        return space;
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
