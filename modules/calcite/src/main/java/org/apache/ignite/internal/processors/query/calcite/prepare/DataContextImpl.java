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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;

/**
 *
 */
class DataContextImpl implements DataContext {
    /** */
    private final JavaTypeFactoryImpl typeFactory;

    /** */
    private final SchemaPlus schema;

    /** */
    private final QueryProvider queryProvider;

    /** */
    private final Map<String, Object> params;

    /**
     * @param params Parameters.
     * @param ctx Query context.
     */
    DataContextImpl(Map<String, Object> params, Context ctx) {
        typeFactory = new JavaTypeFactoryImpl(ctx.unwrap(CalciteQueryProcessor.class).config().getTypeSystem());
        schema = ctx.unwrap(SchemaPlus.class);
        queryProvider = ctx.unwrap(QueryProvider.class);
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return queryProvider;
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        return params.get(name);
    }
}
