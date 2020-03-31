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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.agg;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class AggCallExp implements Externalizable {
    /** */
    private static final byte DISTINCT     = 1;

    /** */
    private static final byte APPROXIMATE  = 1 << 1;

    /** */
    private static final byte IGNORE_NULLS = 1 << 2;

    /** */
    private static final byte HAS_NAME     = 1 << 3;

    /** */
    private DataType type;

    /** */
    private SqlSyntax syntax;

    /** */
    private String function;

    /** */
    private String name;

    /** */
    private int[] args;

    /** */
    private int filterArg;

    /** */
    private byte flags;

    /** */
    public AggCallExp(DataType type, SqlSyntax syntax, String function, String name,
        boolean distinct, boolean approximate, boolean ignoreNulls, int[] args, int filterArg) {
        this.type = type;
        this.syntax = syntax;
        this.function = function;
        this.name = name;
        this.args = args;
        this.filterArg = filterArg;

        byte flags = 0;

        if (distinct)
            flags |= DISTINCT;
        if (approximate)
            flags |= APPROXIMATE;
        if (ignoreNulls)
            flags |= IGNORE_NULLS;

        this.flags = flags;
    }

    public AggCallExp() {
    }

    /** */
    public DataType resultType() {
        return type;
    }

    /** */
    public SqlSyntax syntax() {
        return syntax;
    }

    /** */
    public String function() {
        return function;
    }

    /** */
    public SqlAggFunction sqlOperator(SqlOperatorTable opTable) {
        List<SqlOperator> bag = new MutableSingletonList<>();

        opTable.lookupOperatorOverloads(
            new SqlIdentifier(function, SqlParserPos.ZERO), null,
            syntax, bag, SqlNameMatchers.withCaseSensitive(true));

        return (SqlAggFunction) Objects.requireNonNull(F.first(bag));
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public boolean distinct() {
        return (flags & DISTINCT) == DISTINCT;
    }

    /** */
    public boolean approximate() {
        return (flags & APPROXIMATE) == APPROXIMATE;
    }

    /** */
    public boolean ignoreNulls() {
        return (flags & IGNORE_NULLS) == IGNORE_NULLS;
    }

    /** */
    public int[] args() {
        return args;
    }

    /** */
    public int filterArg() {
        return filterArg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(flags);
        out.writeByte(syntax.ordinal());
        out.writeUTF(function);
        if ((flags & HAS_NAME) == HAS_NAME)
            out.writeUTF(name);
        out.writeObject(type);
        out.writeObject(args);
        out.writeInt(filterArg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        flags = in.readByte();
        syntax = SqlSyntax.values()[in.readByte()];
        function = in.readUTF();
        if ((flags & HAS_NAME) == HAS_NAME)
            name = in.readUTF();
        type = (DataType) in.readObject();
        args = (int[]) in.readObject();
        filterArg = in.readInt();
    }
}
