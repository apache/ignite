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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.jetbrains.annotations.NotNull;

/** An AST node representing option to create index. */
public class IgniteSqlCreateIndexOption extends SqlCall {
    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("IndexOption", SqlKind.OTHER);

    /** Option key. */
    private final SqlLiteral key;

    /** Option value. */
    private final SqlLiteral val;

    /** Creates IgniteSqlCreateIndexOption. */
    public IgniteSqlCreateIndexOption(SqlLiteral key, SqlLiteral val, SqlParserPos pos) {
        super(pos);

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @NotNull @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @NotNull @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(key, val);
    }

    /** {@inheritDoc} */
    @Override public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlCreateIndexOption(key, val, pos);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        val.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> R accept(SqlVisitor<R> visitor) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlCreateIndexOption))
            return litmus.fail("{} != {}", this, node);

        IgniteSqlCreateIndexOption that = (IgniteSqlCreateIndexOption)node;
        if (key != that.key)
            return litmus.fail("{} != {}", this, node);

        return val.equalsDeep(that.val, litmus);
    }

    /**
     * @return Option's key.
     */
    public IgniteSqlCreateIndexOptionEnum key() {
        return key.getValueAs(IgniteSqlCreateIndexOptionEnum.class);
    }

    /**
     * @return Option's value.
     */
    public SqlLiteral value() {
        return val;
    }
}
