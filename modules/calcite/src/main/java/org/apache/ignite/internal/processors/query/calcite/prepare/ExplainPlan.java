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

import java.util.List;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Query explain plan.
 */
public class ExplainPlan implements QueryPlan {
    /** Column name. */
    public static final String PLAN_COL_NAME = "PLAN";

    /** */
    private final List<GridQueryFieldMetadata> fieldsMeta;

    /** */
    private final String plan;

    /** */
    public ExplainPlan(String plan, List<GridQueryFieldMetadata> meta) {
        this.fieldsMeta = meta;
        this.plan = plan;
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.EXPLAIN;
    }

    /** {@inheritDoc} */
    @Override public QueryPlan clone(@Nullable PlanningContext ctx) {
        return this;
    }

    /** */
    public List<GridQueryFieldMetadata> fieldsMeta() {
        return fieldsMeta;
    }

    /** */
    public String plan() {
        return plan;
    }
}
