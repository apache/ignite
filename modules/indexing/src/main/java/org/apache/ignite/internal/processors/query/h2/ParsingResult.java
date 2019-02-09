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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.jetbrains.annotations.Nullable;

/**
 * Result of parsing and splitting SQL from {@link SqlFieldsQuery}.
 */
final class ParsingResult {
    /** New fields query that may be executed right away. */
    private final SqlFieldsQuery qry;

    /** Remaining SQL statements. */
    private final String remainingSql;

    /** Select. */
    private final ParsingResultSelect select;

    /** DML. */
    private final ParsingResultDml dml;

    /** Command. */
    private final ParsingResultCommand cmd;

    /**
     * Constructor.
     *
     * @param qry New query.
     * @param remainingSql Remaining SQL.
     * @param select Select.
     * @param dml DML.
     * @param cmd Command.
     */
    public ParsingResult(
        SqlFieldsQuery qry,
        String remainingSql,
        @Nullable ParsingResultSelect select,
        @Nullable ParsingResultDml dml,
        @Nullable ParsingResultCommand cmd
    ) {
        this.qry = qry;
        this.remainingSql = remainingSql;
        this.select = select;
        this.dml = dml;
        this.cmd = cmd;
    }

    /**
     * @return New fields query that may be executed right away.
     */
    public SqlFieldsQuery query() {
        return qry;
    }

    /**
     * @return Remaining SQL statements.
     */
    public String remainingSql() {
        return remainingSql;
    }

    /**
     * @return SELECT.
     */
    @Nullable public ParsingResultSelect select() {
        return select;
    }

    /**
     * @return DML.
     */
    @Nullable public ParsingResultDml dml() {
        return dml;
    }

    /**
     * @return Command.
     */
    @Nullable public ParsingResultCommand command() {
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
     * @return Number of parameters.
     */
    public int parametersCount() {
        return select != null ? select.parametersCount() : dml != null ? dml.parametersCount() : null;
    }
}
