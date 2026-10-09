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

package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.List;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree for {@code ROLLBACK TO SAVEPOINT name} statement. */
public class IgniteSqlRollbackToSavepoint extends SqlDdl {
    /** */
    protected static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ROLLBACK TO SAVEPOINT", SqlKind.OTHER_DDL);

    /** Savepoint name. */
    private final SqlIdentifier name;

    /**
     * @param pos Parser position.
     * @param name Savepoint name.
     */
    public IgniteSqlRollbackToSavepoint(SqlParserPos pos, SqlIdentifier name) {
        super(OPERATOR, pos);

        this.name = name;
    }

    /** */
    public SqlIdentifier name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return List.of(name);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        name.unparse(writer, leftPrec, rightPrec);
    }
}
