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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.ignite.internal.processors.query.calcite.extension.SqlExtension;
import org.apache.ignite.internal.processors.query.calcite.util.BaseQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * Query mapping context.
 */
public class MappingQueryContext {
    private final BaseQueryContext qctx;

    private final String locNodeId;

    private final long topVer;

    /**
     * Constructor.
     *
     * @param locNodeId Local node identifier.
     * @param topVer    Topology version to map.
     */
    public MappingQueryContext(BaseQueryContext qctx, String locNodeId, long topVer) {
        this.qctx = qctx;
        this.locNodeId = locNodeId;
        this.topVer = topVer;
    }

    /**
     * Get an extensions by it's name.
     *
     * @return An extensions or {@code null} if there is no extension with given name.
     */
    public @Nullable SqlExtension extension(String name) {
        return qctx.extension(name);
    }

    public String localNodeId() {
        return locNodeId;
    }

    public long topologyVersion() {
        return topVer;
    }
}
