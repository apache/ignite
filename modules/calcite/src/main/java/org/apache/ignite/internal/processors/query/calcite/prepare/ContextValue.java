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

import org.apache.calcite.DataContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Helpers to get named parameters from {@link DataContext}
 */
public enum ContextValue {
    QUERY_ID("_query_id", GridCacheVersion.class),
    PLANNER_CONTEXT("_planner_context", PlannerContext.class);

    /** */
    private final String valueName;

    /** */
    private final Class<?> type;

    /**
     * @param valueName Value name.
     * @param type value type.
     */
    ContextValue(String valueName, Class<?> type) {
        this.valueName = valueName;
        this.type = type;
    }

    /**
     * @return valueName.
     */
    public String valueName() {
        return valueName;
    }

    /**
     * @param ctx Data context.
     * @return Parameter value.
     */
    public <T> T get(DataContext ctx) {
        return (T) type.cast(ctx.get(valueName));
    }
}
