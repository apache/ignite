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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/** */
public class Query {
    /** */
    private final QueryRegistry reg;

    /** */
    private final UUID id;

    /** */
    private final Set<RunningFragment> fragments;

    /** */
    protected volatile QueryState state;

    /** */
    private final GridQueryCancel cancel;

    /** */
    public Query(QueryRegistry reg, UUID id, GridQueryCancel cancel) {
        this.id = id;
        this.reg = reg;
        this.cancel = cancel != null ? cancel : new GridQueryCancel();

        fragments = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    /** */
    public UUID id() {
        return id;
    }

    /** */
    public static BaseQueryContext createQueryContext(QueryContext ctx, SchemaPlus schema, IgniteLogger log) {
        return BaseQueryContext.builder()
            .parentContext(Commons.convert(ctx))
            .frameworkConfig(
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .build()
            )
            .logger(log)
            .build();
    }

    /** */
    public enum QueryState {
        /** */
        INIT,

        /** */
        PLANNING,

        /** */
        MAPPING,

        /** */
        RUNNING,

        /** */
        CLOSING,

        /** */
        CLOSED
    }

}
