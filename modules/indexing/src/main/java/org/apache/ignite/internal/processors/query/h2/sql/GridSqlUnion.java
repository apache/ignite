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

import org.h2.command.dml.*;
import org.h2.message.*;
import org.h2.util.*;

import javax.cache.*;
import java.util.*;

/**
 *
 */
public class GridSqlUnion extends GridSqlQuery {
    /** */
    private int unionType;

    /** */
    private GridSqlQuery right;

    /** */
    private GridSqlQuery left;

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder buff = new StringBuilder();

        buff.append('(').append(left.getSQL()).append(')');

        switch (unionType) {
            case SelectUnion.UNION_ALL:
                buff.append("\nUNION ALL\n");
                break;

            case SelectUnion.UNION:
                buff.append("\nUNION\n");
                break;

            case SelectUnion.INTERSECT:
                buff.append("\nINTERSECT\n");
                break;

            case SelectUnion.EXCEPT:
                buff.append("\nEXCEPT\n");
                break;

            default:
                throw new CacheException("type=" + unionType);
        }

        buff.append('(').append(right.getSQL()).append(')');

        if (!sort.isEmpty()) {
            buff.append("\nORDER BY ");

            boolean first = true;

            for (Map.Entry<GridSqlElement, GridSqlSortColumn> entry : sort.entrySet()) {
                if (first)
                    first = false;
                else
                    buff.append(", ");

                GridSqlElement expression = entry.getKey();

                int idx = select.indexOf(expression);

                if (idx >= 0)
                    buff.append(idx + 1);
                else
                    buff.append('=').append(StringUtils.unEnclose(expression.getSQL()));

                GridSqlSortColumn type = entry.getValue();

                if (!type.asc())
                    buff.append(" DESC");

                if (type.nullsFirst())
                    buff.append(" NULLS FIRST");
                else if (type.nullsLast())
                    buff.append(" NULLS LAST");
            }
        }

        if (limit != null)
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limit.getSQL()));

        if (offset != null)
            buff.append(" OFFSET ").append(StringUtils.unEnclose(offset.getSQL()));

        return buff.toString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlUnion clone() {
        GridSqlUnion res = (GridSqlUnion)super.clone();

        res.right = right.clone();
        res.left = left.clone();

        return res;
    }

    /**
     * @return Union type.
     */
    public int unionType() {
        return unionType;
    }

    /**
     * @param unionType New union type.
     */
    public void unionType(int unionType) {
        this.unionType = unionType;
    }

    /**
     * @return Right.
     */
    public GridSqlQuery right() {
        return right;
    }

    /**
     * @param right New right.
     */
    public void right(GridSqlQuery right) {
        this.right = right;
    }

    /**
     * @return Left.
     */
    public GridSqlQuery left() {
        return left;
    }

    /**
     * @param left New left.
     */
    public void left(GridSqlQuery left) {
        this.left = left;
    }
}
