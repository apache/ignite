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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cached two-step query.
 */
public class QueryParserCacheEntry {
    /** Select. */
    private final QueryParserResultSelect select;

    /** DML. */
    private final QueryParserResultDml dml;

    /** Command. */
    private final QueryParserResultCommand cmd;

    /** Metadata for the positional query parameters ('?'). */
    private final List<JdbcParameterMeta> paramsMeta;

    /**
     * Constructor.
     *
     * @param paramsMeta metadata info about positional parameters of the query this record describes.
     * @param select SELECT.
     * @param dml DML.
     * @param cmd Command.
     */
    public QueryParserCacheEntry(
        List<JdbcParameterMeta> paramsMeta,
        @Nullable QueryParserResultSelect select,
        @Nullable QueryParserResultDml dml,
        @Nullable QueryParserResultCommand cmd
    ) {
        assert paramsMeta != null;

        this.paramsMeta = paramsMeta;
        this.select = select;
        this.dml = dml;
        this.cmd = cmd;
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
     * @return Metadata for the positional query parameters ('?').
     */
    public List<JdbcParameterMeta> parametersMeta() {
        return paramsMeta;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryParserCacheEntry.class, this);
    }
}
