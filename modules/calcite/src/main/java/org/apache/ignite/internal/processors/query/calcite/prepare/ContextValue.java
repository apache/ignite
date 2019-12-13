/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 *
 */
public enum ContextValue {
    QUERY_ID("_query_id", GridCacheVersion.class),
    PLANNER_CONTEXT("_planner_context", PlannerContext.class);

    private final String valueName;
    private final Class type;

    ContextValue(String valueName, Class type) {
        this.valueName = valueName;
        this.type = type;
    }

    public String valueName() {
        return valueName;
    }

    public <T> T get(DataContext ctx) {
        return (T) type.cast(ctx.get(valueName));
    }
}
