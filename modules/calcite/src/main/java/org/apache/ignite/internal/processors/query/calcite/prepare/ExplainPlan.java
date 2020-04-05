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

import java.util.List;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Add class description.
 */
public class ExplainPlan implements QueryPlan {

    public static final String PLAN_COL_NAME = "PLAN";

    private final List<GridQueryFieldMetadata> fieldsMeta;

    private final String plan;

    public ExplainPlan(String plan, List<GridQueryFieldMetadata> meta) {
        this.fieldsMeta = meta;
        this.plan = plan;
    }

    @Override public Type type() {
        return Type.EXPLAIN;
    }

    @Override public QueryPlan clone(@Nullable PlanningContext ctx) {
        return this;
    }

    public List<GridQueryFieldMetadata> fieldsMeta() {
        return fieldsMeta;
    }

    public String plan() {
        return plan;
    }
}
