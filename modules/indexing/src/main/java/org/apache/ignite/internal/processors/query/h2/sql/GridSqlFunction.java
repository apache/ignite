/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.UNKNOWN_FUNCTION;

/**
 * Function.
 */
public class GridSqlFunction extends GridSqlElement {
    /** */
    private static final Map<String, GridSqlFunctionType> TYPE_MAP = new HashMap<>();

    /*
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
        super(new ArrayList<GridSqlAst>());

        if (name == null)
            throw new NullPointerException("name");

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

        // We don't need to quote identifier as long as H2 never does so with function names when generating plan SQL.
        // On the other hand, quoting identifiers that also serve as keywords (like CURRENT_DATE() and CURRENT_DATE)
        // turns CURRENT_DATE() into "CURRENT_DATE"(), which is not good.
        buff.append(name);

        if (type == CASE) {
            buff.append(' ').append(child().getSQL());

            for (int i = 1, len = size() - 1; i < len; i += 2) {
                buff.append(" WHEN ").append(child(i).getSQL());
                buff.append(" THEN ").append(child(i + 1).getSQL());
            }

            if ((size() & 1) == 0)
                buff.append(" ELSE ").append(child(size() - 1).getSQL());

            return buff.append(" END").toString();
        }

        buff.append('(');

        switch (type) {
            case CAST:
            case CONVERT:
                assert size() == 1;

                String castType = resultType().sql();

                assert !F.isEmpty(castType) : castType;

                buff.append(child().getSQL());
                buff.append(type == CAST ? " AS " : ",");
                buff.append(castType);

                break;

            case EXTRACT:
                ValueString v = (ValueString)((GridSqlConst)child(0)).value();
                buff.append(v.getString()).append(" FROM ").append(child(1).getSQL());

                break;

            case TABLE:
                for (int i = 0; i < size(); i++) {
                    buff.appendExceptFirst(", ");

                    GridSqlElement e = child(i);

                    // id int = ?, name varchar = ('aaa', 'bbb')
                    buff.append(Parser.quoteIdentifier(((GridSqlAlias)e).alias()))
                        .append(' ')
                        .append(e.resultType().sql())
                        .append('=')
                        .append(e.child().getSQL());
                }

                break;

            default:
                for (int i = 0; i < size(); i++) {
                    buff.appendExceptFirst(", ");
                    buff.append(child(i).getSQL());
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