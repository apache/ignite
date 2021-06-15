/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.sql;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code CREATE ...} statement
 */
public abstract class IgniteSqlCreate extends IgniteSqlCommand {
    /** Whether "IF NOT EXISTS" was specified. */
    public final boolean ifNotExists;

    /** Creates a IgniteSqlCreate. */
    protected IgniteSqlCreate(SqlOperator operator, SqlParserPos pos, boolean ifNotExists) {
        super(operator, pos);
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
