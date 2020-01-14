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
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Parsing result: command.
 */
public class QueryParserResultCommand {
    /** Command (native). */
    private final SqlCommand cmdNative;

    /** Command (H2). */
    private final GridSqlStatement cmdH2;

    /** Whether this is a no-op command. */
    private final boolean noOp;

    /**
     * Constructor.
     *
     * @param cmdNative Command (native).
     * @param cmdH2 Command (H2).
     * @param noOp Whether this is a no-op command.
     */
    public QueryParserResultCommand(@Nullable SqlCommand cmdNative, @Nullable GridSqlStatement cmdH2, boolean noOp) {
        this.cmdNative = cmdNative;
        this.cmdH2 = cmdH2;
        this.noOp = noOp;
    }

    /**
     * @return Command (native).
     */
    @Nullable public SqlCommand commandNative() {
        return cmdNative;
    }

    /**
     * @return Command (H2).
     */
    @Nullable public GridSqlStatement commandH2() {
        return cmdH2;
    }

    /**
     * @return Whether this is a no-op command.
     */
    public boolean noOp() {
        return noOp;
    }
}
