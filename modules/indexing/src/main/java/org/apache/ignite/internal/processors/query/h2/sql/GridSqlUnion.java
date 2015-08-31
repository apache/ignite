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

import javax.cache.CacheException;
import org.h2.command.dml.SelectUnion;
import org.h2.util.StatementBuilder;

/**
 * Select query with UNION.
 */
public class GridSqlUnion extends GridSqlQuery {
    /** */
    private int unionType;

    /** */
    private GridSqlQuery right;

    /** */
    private GridSqlQuery left;

    /** {@inheritDoc} */
    @Override protected int visibleColumns() {
        return left.visibleColumns();
    }

    /** {@inheritDoc} */
    @Override protected GridSqlElement column(int col) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN \n" : "");

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

        getSortLimitSQL(buff);

        return buff.toString();
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