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

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class CreateIndexArguments implements DdlCommandArguments {
    /** */
    private static final long serialVersionUID = 0L;

    /** Overall operation arguments. */
    private final DdlOperationArguments opArgs;

    /** Index params. */
    private final QueryIndex idx;

    // TODO: Table + schema.
    /** Cache name. */
    private final String cacheName;

    /** Ignore operation if index exists. */
    private final boolean ifNotExists;

    /**
     * @param opArgs Overall operation arguments.
     * @param idx Index params.
     * @param cacheName Cache name.
     * @param ifNotExists Ignore operation if index exists.
     */
    CreateIndexArguments(DdlOperationArguments opArgs, QueryIndex idx, String cacheName, boolean ifNotExists) {
        this.opArgs = opArgs;
        this.idx = idx;
        this.cacheName = cacheName;
        this.ifNotExists = ifNotExists;
    }

    /** {@inheritDoc} */
    @Override public DdlOperationArguments getOperationArguments() {
        return opArgs;
    }

    /**
     * @return Index params.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Ignore operation if index exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
