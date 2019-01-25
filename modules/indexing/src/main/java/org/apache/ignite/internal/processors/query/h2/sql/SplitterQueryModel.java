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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.command.dml.SelectUnion;

import java.util.ArrayList;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.LEFT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.RIGHT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.FROM_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion.LEFT_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion.RIGHT_CHILD;

/**
 * Simplified tree-like model for a query.
 * - SELECT : All the children are list of joined query models in the FROM clause.
 * - UNION  : All the children are united left and right query models.
 * - TABLE and FUNCTION : Never have child models.
 */
public final class SplitterQueryModel extends ArrayList<SplitterQueryModel> {
    /** */
    @GridToStringInclude
    private final SplitterQueryModelType type;

    /** */
    private final GridSqlAlias uniqueAlias;

    /** */
    private final GridSqlAst parent;

    /** */
    private final int childIdx;

    /** If it is a SELECT and we need to split it. Makes sense only for type SELECT. */
    @GridToStringInclude
    private boolean needSplit;

    /** If we have a child SELECT that we should split. */
    @GridToStringInclude
    private boolean needSplitChild;

    /** If this is UNION ALL. Makes sense only for type UNION.*/
    private boolean unionAll = true;

    /**
     * Constructor (no split).
     *
     * @param type Type.
     * @param parent Parent element.
     * @param childIdx Child index.
     * @param uniqueAlias Unique parent alias of the current element.
     *     May be {@code null} for selects inside of unions or top level queries.
     */
    public SplitterQueryModel(
        SplitterQueryModelType type,
        GridSqlAst parent,
        int childIdx,
        GridSqlAlias uniqueAlias
    ) {
        this(type, parent, childIdx, uniqueAlias, false);
    }

    /**
     * Constructor.
     *
     * @param type Type.
     * @param parent Parent element.
     * @param childIdx Child index.
     * @param uniqueAlias Unique parent alias of the current element.
     *     May be {@code null} for selects inside of unions or top level queries.
     * @param needSplit Need split flag.
     */
    public SplitterQueryModel(
        SplitterQueryModelType type,
        GridSqlAst parent,
        int childIdx,
        GridSqlAlias uniqueAlias,
        boolean needSplit
    ) {
        this.type = type;
        this.parent = parent;
        this.childIdx = childIdx;
        this.uniqueAlias = uniqueAlias;
        this.needSplit = needSplit;
    }

    /**
     * @return Type.
     */
    public SplitterQueryModelType type() {
        return type;
    }

    /**
     * @return {@code true} If this is a SELECT or UNION query model.
     */
    public boolean isQuery() {
        return type == SplitterQueryModelType.SELECT || type == SplitterQueryModelType.UNION;
    }

    /**
     * @return Unique alias.
     */
    public GridSqlAlias uniqueAlias() {
        return uniqueAlias;
    }

    /**
     * @return Parent AST element.
     */
    public GridSqlAst parent() {
        return parent;
    }

    /**
     * @return Child index.
     */
    public int childIndex() {
        return childIdx;
    }

    /**
     * @return The actual AST element for this model.
     */
    @SuppressWarnings("TypeParameterHidesVisibleType")
    public <X extends GridSqlAst> X ast() {
        return parent.child(childIdx);
    }

    /**
     * @return Whether split is needed.
     */
    public boolean needSplit() {
        return needSplit;
    }

    /**
     * @return Whether split of children is needed.
     */
    public boolean needSplitChild() {
        return needSplitChild;
    }

    /**
     * Force split flag on a model.
     */
    public void forceSplit() {
        if (type == SplitterQueryModelType.SELECT) {
            assert !needSplitChild;

            needSplit = true;
        }
        else if (type == SplitterQueryModelType.UNION) {
            needSplitChild = true;

            // Mark all the selects in the UNION to be splittable.
            for (SplitterQueryModel s : this) {
                assert s.type() == SplitterQueryModelType.SELECT : s.type();

                s.needSplit = true;
            }
        }
        else
            throw new IllegalStateException("Type: " + type);
    }

    /**
     * @param unionAll UNION ALL flag.
     */
    @SuppressWarnings("SameParameterValue")
    public void unionAll(boolean unionAll) {
        this.unionAll = unionAll;
    }

    /**
     * Prepare query model.
     *
     * @param prnt Parent AST element.
     * @param childIdx Child index.
     * @param uniqueAlias Unique parent alias of the current element.
     */
    public void buildQueryModel(GridSqlAst prnt, int childIdx, GridSqlAlias uniqueAlias) {
        GridSqlAst child = prnt.child(childIdx);

        assert child != null;

        if (child instanceof GridSqlSelect) {
            SplitterQueryModel model = new SplitterQueryModel(SplitterQueryModelType.SELECT, prnt, childIdx,
                uniqueAlias);

            add(model);

            model.buildQueryModel(child, FROM_CHILD, null);
        }
        else if (child instanceof GridSqlUnion) {
            SplitterQueryModel model;

            // We will collect all the selects into a single UNION model.
            if (type == SplitterQueryModelType.UNION)
                model = this;
            else {
                model = new SplitterQueryModel(SplitterQueryModelType.UNION, prnt, childIdx, uniqueAlias);

                add(model);
            }

            if (((GridSqlUnion)child).unionType() != SelectUnion.UnionType.UNION_ALL)
                model.unionAll(false);

            model.buildQueryModel(child, LEFT_CHILD, null);
            model.buildQueryModel(child, RIGHT_CHILD, null);
        }
        else {
            // Here we must be in FROM clause of the SELECT.
            assert type == SplitterQueryModelType.SELECT : type;

            if (child instanceof GridSqlAlias)
                buildQueryModel(child, 0, (GridSqlAlias)child);
            else if (child instanceof GridSqlJoin) {
                buildQueryModel(child, LEFT_TABLE_CHILD, uniqueAlias);
                buildQueryModel(child, RIGHT_TABLE_CHILD, uniqueAlias);
            }
            else {
                // Here we must be inside of generated unique alias for FROM clause element.
                assert prnt == uniqueAlias: prnt.getClass();

                if (child instanceof GridSqlTable)
                    add(new SplitterQueryModel(SplitterQueryModelType.TABLE, prnt, childIdx, uniqueAlias));
                else if (child instanceof GridSqlSubquery)
                    buildQueryModel(child, 0, uniqueAlias);
                else if (child instanceof GridSqlFunction)
                    add(new SplitterQueryModel(SplitterQueryModelType.FUNCTION, prnt, childIdx, uniqueAlias));
                else
                    throw new IllegalStateException("Unknown child type: " + child.getClass());
            }
        }
    }

    /**
     * Analyze query model, setting split flags as needed.
     *
     * @param collocatedGrpBy Collocated GROUP BY flag.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void analyzeQueryModel(boolean collocatedGrpBy) {
        if (!isQuery())
            return;

        // Process all the children at the beginning: depth first analysis.
        for (int i = 0; i < size(); i++) {
            SplitterQueryModel child = get(i);

            child.analyzeQueryModel(collocatedGrpBy);

            // Pull up information about the splitting child.
            if (child.needSplit || child.needSplitChild)
                needSplitChild = true; // We have a child to split.
        }

        if (type == SplitterQueryModelType.SELECT) {
            // We may need to split the SELECT only if it has no splittable children,
            // because only the downmost child can be split, the parents will be the part of
            // the reduce query.
            if (!needSplitChild)
                needSplit = needSplitSelect(ast(), collocatedGrpBy); // Only SELECT can have this flag in true.
        }
        else if (type == SplitterQueryModelType.UNION) {
            // If it is not a UNION ALL, then we have to split because otherwise we can produce duplicates or
            // wrong results for UNION DISTINCT, EXCEPT, INTERSECT queries.
            if (!needSplitChild && (!unionAll || ((GridSqlUnion)ast()).hasOffsetLimit()))
                needSplitChild = true;

            // If we have to split some child SELECT in this UNION, then we have to enforce split
            // for all other united selects, because this UNION has to be a part of the reduce query,
            // thus each SELECT needs to have a reduce part for this UNION, but the whole SELECT can not
            // be a reduce part (usually).
            if (needSplitChild) {
                for (int i = 0; i < size(); i++) {
                    SplitterQueryModel child = get(i);

                    assert child.type() == SplitterQueryModelType.SELECT : child.type();

                    if (!child.needSplitChild && !child.needSplit)
                        child.needSplit = true;
                }
            }
        }
        else
            throw new IllegalStateException("Type: " + type);
    }

    /**
     * @param select Select to check.
     * @param collocatedGrpBy Collocated GROUP BY flag.
     * @return {@code true} If we need to split this select.
     */
    private static boolean needSplitSelect(GridSqlSelect select, boolean collocatedGrpBy) {
        if (select.distinct())
            return true;

        if (select.hasOffsetLimit())
            return true;

        if (collocatedGrpBy)
            return false;

        if (select.groupColumns() != null)
            return true;

        for (int i = 0; i < select.allColumns(); i++) {
            if (SplitterUtils.hasAggregates(select.column(i)))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SplitterQueryModel.class, this);
    }
}
