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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Result of parsing and splitting SQL from {@link SqlFieldsQuery}.
 */
public class QueryParserResult {
    /** Query descriptor. */
    private final QueryDescriptor qryDesc;

    /** Query parameters. */
    private final QueryParameters qryParams;

    /** Remaining query. */
    private final SqlFieldsQuery remainingQry;

    /** Metadata for the positional query parameters ('?'). */
    private final List<JdbcParameterMeta> paramsMeta;

    /** Select. */
    private final QueryParserResultSelect select;

    /** DML. */
    private final QueryParserResultDml dml;

    /** Command. */
    private final QueryParserResultCommand cmd;

    /**
     * Constructor.
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param remainingQry Remaining query.
     * @param paramsMeta metadata info about positional parameters of current parsed query (not includes remainingSql).
     * @param select Select.
     * @param dml DML.
     * @param cmd Command.
     */
    public QueryParserResult(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        SqlFieldsQuery remainingQry,
        @NotNull List<JdbcParameterMeta> paramsMeta,
        @Nullable QueryParserResultSelect select,
        @Nullable QueryParserResultDml dml,
        @Nullable QueryParserResultCommand cmd
    ) {
        assert paramsMeta != null;

        this.qryDesc = qryDesc;
        this.qryParams = qryParams;
        this.remainingQry = remainingQry;
        this.paramsMeta = paramsMeta;
        this.select = select;
        this.dml = dml;
        this.cmd = cmd;
    }

    /**
     * @return Query descriptor.
     */
    public QueryDescriptor queryDescriptor() {
        return qryDesc;
    }

    /**
     * @return Query parameters.
     */
    public QueryParameters queryParameters() {
        return qryParams;
    }

    /**
     * @return Remaining query.
     */
    @Nullable public SqlFieldsQuery remainingQuery() {
        return remainingQry;
    }

    /**
     * @return SELECT.
     */
    @Nullable public QueryParserResultSelect select() {
        return select;
    }

    /**
     * @return DML.
     */
    @Nullable public QueryParserResultDml dml() {
        return dml;
    }

    /**
     * @return Command.
     */
    @Nullable public QueryParserResultCommand command() {
        return cmd;
    }

    /**
     * @return Check whether this is SELECT.
     */
    public boolean isSelect() {
        return select != null;
    }

    /**
     * @return Check whether this is DML.
     */
    public boolean isDml() {
        return dml != null;
    }

    /**
     * @return Check whether this is a command.
     */
    public boolean isCommand() {
        return cmd != null;
    }

    /**
     * @return Number of current statement parameters.
     */
    public int parametersCount() {
        return paramsMeta.size();
    }

    /**
     * @return Descriptions of each query parameter of current statement. Empty list in case of native command, never
     * {@code null}.
     */
    @NotNull public List<JdbcParameterMeta> parametersMeta() {
        return paramsMeta;
    }
}
