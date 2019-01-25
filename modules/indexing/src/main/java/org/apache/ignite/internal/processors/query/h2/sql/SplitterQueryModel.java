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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SplitterQueryModel.class, this);
    }
}
