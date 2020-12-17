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

package org.apache.ignite.internal.processors.query.h2.index.client;

import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexName;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Define index for filtered or client node.
 */
public class ClientIndexDefinition implements IndexDefinition {
    /** */
    private final QueryIndexSchema schema;

    /** */
    private final int cfgInlineSize;

    /** */
    private final IndexName idxName;

    /** */
    public ClientIndexDefinition(IndexName idxName, QueryIndexSchema schema, int cfgInlineSize) {
        this.idxName = idxName;
        this.schema = schema;
        this.cfgInlineSize = cfgInlineSize;
    }

    /** */
    public int getCfgInlineSize() {
        return cfgInlineSize;
    }

    /** */
    public QueryIndexSchema getSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public IndexName getIdxName() {
        return idxName;
    }
}
