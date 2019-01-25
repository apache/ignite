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

/**
 * AND condition for splitter.
 */
public class SplitterAndCondition {
    /** Parent node. */
    private final GridSqlAst parent;

    /** Child index. */
    private final int childIdx;

    /**
     * @param parent Parent element.
     * @param childIdx Child index.
     */
    public SplitterAndCondition(GridSqlAst parent, int childIdx) {
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
