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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.jetbrains.annotations.Nullable;

/**
 * Parsing result for DML statement.
 */
public class QueryParserResultDml {
    /** Command. */
    private final GridSqlStatement stmt;

    /** Number of parameters. */
    private final List<JdbcParameterMeta> paramsMeta;

    /** MVCC enabled flag. */
    private final boolean mvccEnabled;

    /** Streamer table. */
    private final GridH2Table streamTbl;

    /** Update plan. */
    private final UpdatePlan plan;

    /**
     * Constructor.
     *
     * @param stmt Command.
     * @param paramsMeta Description of positional query parameters.
     * @param mvccEnabled Whether MVCC is enabled.
     * @param streamTbl Streamer table.
     * @param plan Update plan.
     */
    public QueryParserResultDml(
        GridSqlStatement stmt,
        List<JdbcParameterMeta> paramsMeta,
        boolean mvccEnabled,
        @Nullable GridH2Table streamTbl,
        UpdatePlan plan
    ) {
        this.stmt = stmt;
        this.paramsMeta = paramsMeta;
        this.mvccEnabled = mvccEnabled;
        this.streamTbl = streamTbl;
        this.plan = plan;
    }

    /**
     * @return Command.
     */
    public GridSqlStatement statement() {
        return stmt;
    }

    /**
     * @return MVCC enabled.
     */
    public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /**
     * @return Streamer table.
     */
    @Nullable public GridH2Table streamTable() {
        return streamTbl;
    }

    /**
     * @return Whether statement can be used in streaming.
     */
    public boolean streamable() {
        return streamTbl != null;
    }

    /**
     * @return Number of parameters.
     */
    public List<JdbcParameterMeta> parametersMeta() {
        return paramsMeta;
    }

    /**
     * @return Update plan.
     */
    public UpdatePlan plan() {
        return plan;
    }
}
