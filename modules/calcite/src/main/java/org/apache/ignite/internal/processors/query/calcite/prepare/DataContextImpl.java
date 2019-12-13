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

import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

/**
 *
 */
public class DataContextImpl implements DataContext {
    /** */
    private final PlannerContext ctx;

    /** */
    private final Map<String, Object> params;

    /**
     * @param params Parameters.
     * @param ctx Query context.
     */
    public DataContextImpl(Map<String, Object> params, PlannerContext ctx) {
        this.params = params;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return ctx.schema();
    }

    /** {@inheritDoc} */
    @Override public JavaTypeFactory getTypeFactory() {
        return ctx.typeFactory();
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return ctx.queryProvider();
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        if (ContextValue.PLANNER_CONTEXT.valueName().equals(name))
            return ctx;

        return params.get(name);
    }
}
