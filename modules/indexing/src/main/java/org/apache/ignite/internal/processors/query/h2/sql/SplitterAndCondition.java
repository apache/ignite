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

import java.util.List;

/**
 * AND condition for splitter.
 */
public class SplitterAndCondition {
    /** Parent node. */
    private final GridSqlAst parent;

    /** Child index. */
    private final int childIdx;

    /**
     * Collect and conditions from the given element.
     *
     * @param res Conditions in AND.
     * @param parent Parent element.
     * @param childIdx Child index.
     */
    public static void collectAndConditions(List<SplitterAndCondition> res, GridSqlAst parent, int childIdx) {
        GridSqlAst child = parent.child(childIdx);

        if (child instanceof GridSqlOperation) {
            GridSqlOperation op = (GridSqlOperation)child;

            if (op.operationType() == GridSqlOperationType.AND) {
                collectAndConditions(res, op, 0);
                collectAndConditions(res, op, 1);

                return;
            }
        }

        if (!SplitterUtils.isTrue(child))
            res.add(new SplitterAndCondition(parent, childIdx));
    }

    /**
     * @param parent Parent element.
     * @param childIdx Child index.
     */
    private SplitterAndCondition(GridSqlAst parent, int childIdx) {
        this.parent = parent;
        this.childIdx = childIdx;
    }

    /**
     * @return The actual AST element for this expression.
     */
    @SuppressWarnings("TypeParameterHidesVisibleType")
    public <X extends GridSqlAst> X ast() {
        return parent.child(childIdx);
    }

    /**
     * @return Parent node.
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
}
