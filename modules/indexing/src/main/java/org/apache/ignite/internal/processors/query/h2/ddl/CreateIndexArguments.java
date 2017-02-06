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

import java.util.UUID;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Arguments for {@code CREATE INDEX}.
 */
public class CreateIndexArguments extends DdlOperationArguments {
    /** Index. */
    public final QueryIndex idx;

    /** Ignore operation if index exists. */
    public final boolean ifNotExists;

    /**
     * @param sndNodeId ID of node that initiated this operation.
     * @param cacheName Cache name.
     * @param idx Index params.
     * @param ifNotExists Ignore operation  if index exists.
     */
    public CreateIndexArguments(UUID sndNodeId, IgniteUuid opId, String cacheName, QueryIndex idx, boolean ifNotExists) {
        super(sndNodeId, opId, cacheName, DdlOperationType.CREATE_INDEX);
        this.idx = idx;
        this.ifNotExists = ifNotExists;
    }
}
