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
    private final int paramsCnt;

    /** MVCC enabled flag. */
    private final boolean mvccEnabled;

    /** Streamer table. */
    private final GridH2Table streamTbl;

    /**
     * Constructor.
     *
     * @param stmt Command.
     * @param paramsCnt Number of parameters.
     * @param mvccEnabled Whether MVCC is enabled.
     * @param streamTbl Streamer table.
     */
    public QueryParserResultDml(
        GridSqlStatement stmt,
        int paramsCnt,
        boolean mvccEnabled,
        @Nullable GridH2Table streamTbl
    ) {
        this.stmt = stmt;
        this.paramsCnt = paramsCnt;
        this.mvccEnabled = mvccEnabled;
        this.streamTbl = streamTbl;
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
    public int parametersCount() {
        return paramsCnt;
    }
}
