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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.Parser;
import org.h2.util.StatementBuilder;
import org.h2.value.ValueString;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CASE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CAST;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CONVERT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.UNKNOWN_FUNCTION;

/**
 * Function.
 */
public class GridSqlFunction extends GridSqlElement {
    /** */
    private static final Map<String, GridSqlFunctionType> TYPE_MAP = new HashMap<>();

    /**
     *
     */
    static {
        for (GridSqlFunctionType type : GridSqlFunctionType.values())
            TYPE_MAP.put(type.name(), type);
    }

    /** */
    private final String schema;

    /** */
    private final String name;

    /** */
    protected final GridSqlFunctionType type;

    /**
     * @param type Function type.
     */
    public GridSqlFunction(GridSqlFunctionType type) {
        this(null, type, type.functionName());
    }

    /**
     * @param schema Schema.
     * @param type Type.
     * @param name Name.
     */
    private GridSqlFunction(String schema, GridSqlFunctionType type, String name) {
        super(new ArrayList<GridSqlElement>());

        if (name == null)
            throw new NullPointerException();

        if (type == null)
            type = UNKNOWN_FUNCTION;

        this.schema = schema;
        this.name = name;
        this.type = type;
    }

    /**
     * @param schema Schema.
     * @param name Name.
     */
    public GridSqlFunction(String schema, String name) {
        this(schema, TYPE_MAP.get(name), name);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        if (schema != null)
            buff.append(Parser.quoteIdentifier(schema)).append('.');

        buff.append(Parser.quoteIdentifier(name));

        if (type == CASE) {
            buff.append(' ').append(child().getSQL());

            for (int i = 1, len = children.size() - 1; i < len; i += 2) {
                buff.append(" WHEN ").append(child(i).getSQL());
                buff.append(" THEN ").append(child(i + 1).getSQL());
            }

            if ((children.size() & 1) == 0)
                buff.append(" ELSE ").append(child(children.size() - 1).getSQL());

            return buff.append(" END").toString();
        }

        buff.append('(');

        if (type == CAST) {
            String castType = resultType().sql();

            assert !F.isEmpty(castType) : castType;
            assert size() == 1;

            buff.append(child().getSQL()).append(" AS ").append(castType);
        }
        else if (type == CONVERT) {
            String castType = resultType().sql();

            assert !F.isEmpty(castType) : castType;
            assert size() == 1;

            buff.append(child().getSQL()).append(',').append(castType);
        }
        else if (type == GridSqlFunctionType.EXTRACT) {
            ValueString v = (ValueString)((GridSqlConst)child(0)).value();
            buff.append(v.getString()).append(" FROM ").append(child(1).getSQL());
        }
        else {
            for (GridSqlElement e : children) {
                buff.appendExceptFirst(", ");
                buff.append(e.getSQL());
            }
        }

        return buff.append(')').toString();
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Type.
     */
    public GridSqlFunctionType type() {
        return type;
    }
}