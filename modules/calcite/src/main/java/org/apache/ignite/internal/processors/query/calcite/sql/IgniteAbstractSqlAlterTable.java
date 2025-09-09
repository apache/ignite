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

import java.util.Objects;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code ALTER TABLE } statement
 */
public abstract class IgniteAbstractSqlAlterTable extends SqlDdl {
    /** */
    protected final SqlIdentifier name;

    /** */
    protected final boolean ifExists;

    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

    /** */
    protected IgniteAbstractSqlAlterTable(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName) {
        super(OPERATOR, pos);
        this.ifExists = ifExists;
        name = Objects.requireNonNull(tblName, "table name");
    }

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        if (ifExists)
            writer.keyword("IF EXISTS");

        name.unparse(writer, leftPrec, rightPrec);

        unparseAlterTableOperation(writer, leftPrec, rightPrec);
    }

    /**
     * Unparse rest of the ALTER TABLE command.
     */
    protected abstract void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec);

    /**
     * @return Name of the object.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * @return Whether the IF EXISTS is specified.
     */
    public boolean ifExists() {
        return ifExists;
    }
}
