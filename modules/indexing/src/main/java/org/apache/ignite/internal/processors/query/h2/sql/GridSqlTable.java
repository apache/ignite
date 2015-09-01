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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Collections;
import org.h2.command.Parser;
import org.jetbrains.annotations.Nullable;

/**
 * Table with optional schema.
 */
public class GridSqlTable extends GridSqlElement {
    /** */
    private final String schema;

    /** */
    private final String tblName;

    /**
     * @param schema Schema.
     * @param tblName Table name.
     */
    public GridSqlTable(@Nullable String schema, String tblName) {
        super(Collections.<GridSqlElement>emptyList());

        this.schema = schema;
        this.tblName = tblName;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        if (schema == null)
            return Parser.quoteIdentifier(tblName);

        return Parser.quoteIdentifier(schema) + '.' + Parser.quoteIdentifier(tblName);
    }

    /**
     * @return Schema.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }
}