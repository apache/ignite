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

import java.util.ArrayList;

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
     * @param type Type.
     * @param parent Parent element.
     * @param childIdx Child index.
     * @param uniqueAlias Unique parent alias of the current element.
     *                    May be {@code null} for selects inside of unions or top level queries.
     */
    public SplitterQueryModel(SplitterQueryModelType type, GridSqlAst parent, int childIdx, GridSqlAlias uniqueAlias) {
        this.type = type;
        this.parent = parent;
        this.childIdx = childIdx;
        this.uniqueAlias = uniqueAlias;
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
     * @param needSplit Whether split is needed.
     */
    public void needSplit(boolean needSplit) {
        this.needSplit = needSplit;
    }

    /**
     * @return Whether split of children is needed.
     */
    public boolean needSplitChild() {
        return needSplitChild;
    }

    /**
     * @param needSplitChild Whether split of children is needed.
     */
    public void needSplitChild(boolean needSplitChild) {
        this.needSplitChild = needSplitChild;
    }

    /**
     * Set split needed flag.
     */
    public void setNeedSplit() {
        if (type == SplitterQueryModelType.SELECT) {
            assert !needSplitChild;

            needSplit(true);
        }
        else if (type == SplitterQueryModelType.UNION) {
            needSplitChild(true);

            // Mark all the selects in the UNION to be splittable.
            for (SplitterQueryModel s : this) {
                assert s.type() == SplitterQueryModelType.SELECT : s.type();

                s.needSplit(true);
            }
        }
        else
            throw new IllegalStateException("Type: " + type);
    }

    /**
     * @return UNION ALL flag.
     */
    public boolean unionAll() {
        return unionAll;
    }

    /**
     * @param unionAll UNION ALL flag.
     */
    @SuppressWarnings("SameParameterValue")
    public void unionAll(boolean unionAll) {
        this.unionAll = unionAll;
    }

    /**
     * Analyze query model.
     *
     * @param qrym Query model.
     * @param collocatedGrpBy Collocated GROUP BY flag.
     */
    // TODO: Document
    // TODO: Make instance-based
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static void analyzeQueryModel(SplitterQueryModel qrym, boolean collocatedGrpBy) {
        if (!qrym.isQuery())
            return;

        // Process all the children at the beginning: depth first analysis.
        for (int i = 0; i < qrym.size(); i++) {
            SplitterQueryModel child = qrym.get(i);

            analyzeQueryModel(child, collocatedGrpBy);

            // Pull up information about the splitting child.
            if (child.needSplit() || child.needSplitChild())
                qrym.needSplitChild(true); // We have a child to split.
        }

        if (qrym.type() == SplitterQueryModelType.SELECT) {
            // We may need to split the SELECT only if it has no splittable children,
            // because only the downmost child can be split, the parents will be the part of
            // the reduce query.
            if (!qrym.needSplitChild())
                qrym.needSplit(needSplitSelect(qrym.ast(), collocatedGrpBy)); // Only SELECT can have this flag in true.
        }
        else if (qrym.type() == SplitterQueryModelType.UNION) {
            // If it is not a UNION ALL, then we have to split because otherwise we can produce duplicates or
            // wrong results for UNION DISTINCT, EXCEPT, INTERSECT queries.
            if (!qrym.needSplitChild() && (!qrym.unionAll() || qrym.<GridSqlUnion>ast().hasOffsetLimit()))
                qrym.needSplitChild(true);

            // If we have to split some child SELECT in this UNION, then we have to enforce split
            // for all other united selects, because this UNION has to be a part of the reduce query,
            // thus each SELECT needs to have a reduce part for this UNION, but the whole SELECT can not
            // be a reduce part (usually).
            if (qrym.needSplitChild()) {
                for (int i = 0; i < qrym.size(); i++) {
                    SplitterQueryModel child = qrym.get(i);

                    assert child.type() == SplitterQueryModelType.SELECT : child.type();

                    if (!child.needSplitChild() && !child.needSplit())
                        child.needSplit(true);
                }
            }
        }
        else
            throw new IllegalStateException("Type: " + qrym.type());
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
