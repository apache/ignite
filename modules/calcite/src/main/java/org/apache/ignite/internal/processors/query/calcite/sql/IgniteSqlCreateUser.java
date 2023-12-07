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

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code CREATE USER} statement
 */
public class IgniteSqlCreateUser extends SqlCreate {
    /** */
    private final SqlIdentifier user;

    /** */
    private final SqlLiteral pwd;

    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE USER", SqlKind.OTHER_DDL);

    /** */
    public IgniteSqlCreateUser(SqlParserPos pos, SqlIdentifier user, SqlLiteral pwd) {
        super(OPERATOR, pos, false, false);
        this.user = Objects.requireNonNull(user, "user");
        this.pwd = Objects.requireNonNull(pwd, "password");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(user, pwd);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        user.unparse(writer, 0, 0);

        writer.keyword("WITH PASSWORD");

        pwd.unparse(writer, 0, 0);
    }

    /**
     * @return Username.
     */
    public SqlIdentifier user() {
        return user;
    }

    /**
     * @return Password
     */
    public String password() {
        return pwd.getValueAs(String.class);
    }
}
