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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class IgniteSqlOption<E extends Enum<E>> extends SqlCall {
    /** Option key. */
    protected final SqlLiteral key;

    /** Option value. */
    protected final SqlNode value;

    /** */
    private final Class<E> enumCls;

    /** */
    protected IgniteSqlOption(SqlLiteral key, SqlNode value, SqlParserPos pos, Class<E> enumCls) {
        super(pos);

        this.enumCls = enumCls;
        this.key = key;
        this.value = value;
    }

    /** {@inheritDoc} */
    @NotNull @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(key, value);
    }

    /** {@inheritDoc} */
    @Override public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlCreateTableOption(key, value, pos);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (node == this) {
            return true;
        }

        if (!(node instanceof IgniteSqlOption)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlOption<E> that = (IgniteSqlOption<E>)node;

        if (!getOperator().getName().equalsIgnoreCase(that.getOperator().getName())) {
            return litmus.fail("{} != {}", this, node);
        }

        if (key != that.key)
            return litmus.fail("{} != {}", this, node);

        return value.equalsDeep(that.value, litmus);
    }

    /**
     * @return Option's key.
     */
    public E key() {
        return key.getValueAs(enumCls);
    }

    /**
     * @return Option's value.
     */
    public SqlNode value() {
        return value;
    }

    /** */
    protected static interface OptionFactory<T> {
        /** */
        T create(SqlLiteral key, SqlNode value, SqlParserPos pos);
    }

    /**
     * Parse option list. Used for H2-based "ANALYZE" syntax.
     *
     * @return Parsed option list.
     */
    protected static <T extends IgniteSqlOption<E>, E extends Enum<E>> SqlNodeList parseOptionList(
        String opts,
        SqlParserPos pos,
        OptionFactory<T> factory,
        Class<E> enumCls
    ) {
        SqlNodeList list = new SqlNodeList(pos);

        String[] pairs = opts.split(",");

        for (String pair : pairs) {
            String[] keyVal = pair.split("=");

            if (keyVal.length != 2)
                throw SqlUtil.newContextException(pos, IgniteResource.INSTANCE.cannotParsePair(pair));

            String key = keyVal[0].trim();
            String val = keyVal[1].trim();

            E optionKey = E.valueOf(enumCls, key.toUpperCase());

            if (optionKey == null)
                throw SqlUtil.newContextException(pos, IgniteResource.INSTANCE.illegalOption(key));

            list.add(factory.create(
                SqlLiteral.createSymbol(optionKey, pos),
                new SqlIdentifier(val, pos),
                pos
            ));
        }

        return list;
    }
}
