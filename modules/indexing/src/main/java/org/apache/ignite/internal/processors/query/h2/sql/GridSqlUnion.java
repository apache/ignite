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
    public static final int LEFT_CHILD = 2;

    /** */
    public static final int RIGHT_CHILD = 3;

    /** */
    private SelectUnion.UnionType unionType;

    /** */
    private GridSqlQuery right;

    /** */
    private GridSqlQuery left;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <E extends GridSqlAst> E child(int childIdx) {
        if (childIdx < LEFT_CHILD)
            return super.child(childIdx);

        switch (childIdx) {
            case LEFT_CHILD:
                assert left != null;

                return (E)left;

            case RIGHT_CHILD:
                assert right != null;

                return (E)right;

            default:
                throw new IllegalStateException("Child index: " + childIdx);
        }
    }

    /** {@inheritDoc} */
    @Override public <E extends GridSqlAst> void child(int childIdx, E child) {
        if (childIdx < LEFT_CHILD) {
            super.child(childIdx, child);

            return;
        }

        switch (childIdx) {
            case LEFT_CHILD:
                left = (GridSqlQuery)child;

                break;

            case RIGHT_CHILD:
                right = (GridSqlQuery)child;

                break;

            default:
                throw new IllegalStateException("Child index: " + childIdx);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 4; // OFFSET + LIMIT + LEFT + RIGHT
    }

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

        switch (unionType()) {
            case UNION_ALL:
                buff.append("\nUNION ALL\n");
                break;

            case UNION:
                buff.append("\nUNION\n");
                break;

            case INTERSECT:
                buff.append("\nINTERSECT\n");
                break;

            case EXCEPT:
                buff.append("\nEXCEPT\n");
                break;

            default:
                throw new CacheException("type=" + unionType);
        }

        buff.append('(').append(right.getSQL()).append(')');

        getSortLimitSQL(buff);

        return buff.toString();
    }

    /** {@inheritDoc} */
    @Override public boolean skipMergeTable() {
        return unionType() == SelectUnion.UnionType.UNION_ALL && sort().isEmpty() &&
            offset() == null && limit() == null &&
            left().skipMergeTable() && right().skipMergeTable();
    }

    /**
     * @return Union type.
     */
    public SelectUnion.UnionType unionType() {
        return unionType;
    }

    /**
     * @param unionType New union type.
     */
    public void unionType(SelectUnion.UnionType unionType) {
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
