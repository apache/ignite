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

/** An AST node representing option to create table with. */
public class IgniteSqlCreateTableOption extends SqlCall {
    /**
     *
     */
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("TableOption", SqlKind.OTHER);
    
    /** Option key. */
    private final SqlLiteral key;
    
    /** Option value. */
    private final SqlNode value;
    
    /** Creates IgniteSqlCreateTableOption. */
    public IgniteSqlCreateTableOption(SqlLiteral key, SqlNode value, SqlParserPos pos) {
        super(pos);
        
        this.key = key;
        this.value = value;
    }
    
    /** {@inheritDoc} */
    @NotNull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }
    
    /** {@inheritDoc} */
    @NotNull
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, value);
    }
    
    /** {@inheritDoc} */
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlCreateTableOption(key, value, pos);
    }
    
    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }
    
    /** {@inheritDoc} */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException();
    }
    
    /** {@inheritDoc} */
    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        throw new UnsupportedOperationException();
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlCreateTableOption)) {
            return litmus.fail("{} != {}", this, node);
        }
        
        IgniteSqlCreateTableOption that = (IgniteSqlCreateTableOption) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }
        
        return value.equalsDeep(that.value, litmus);
    }
    
    /**
     * @return Option's key.
     */
    public IgniteSqlCreateTableOptionEnum key() {
        return key.getValueAs(IgniteSqlCreateTableOptionEnum.class);
    }
    
    /**
     * @return Option's value.
     */
    public SqlNode value() {
        return value;
    }
}
