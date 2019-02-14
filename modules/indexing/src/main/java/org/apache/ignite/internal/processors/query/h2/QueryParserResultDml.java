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

import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.h2.command.Prepared;

/**
 * Parsing result for DML statement.
 */
public class QueryParserResultDml {
    /** Command. */
    private final GridSqlStatement stmt;

    /** MVCC enabled flag. */
    private final boolean mvccEnabled;

    /**
     * Constructor.
     *
     * @param stmt Command.
     */
    public QueryParserResultDml(GridSqlStatement stmt, boolean mvccEnabled) {
        this.stmt = stmt;
        this.mvccEnabled = mvccEnabled;
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
}
