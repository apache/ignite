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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IndexConditions {
    /** */
    private final List<RexNode> lowerCond;

    /** */
    private final List<RexNode> upperCond;

    /** */
    private final List<RexNode> lowerBound;

    /** */
    private final List<RexNode> upperBound;

    /** */
    public IndexConditions() {
        this(null, null, null, null);
    }

    /**
     */
    public IndexConditions(
        @Nullable List<RexNode> lowerCond,
        @Nullable List<RexNode> upperCond,
        @Nullable List<RexNode> lowerBound,
        @Nullable List<RexNode> upperBound
    ) {
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerCondition() {
        return lowerCond;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperCondition() {
        return upperCond;
    }

    /**
     * @return Lower index bounds (a row with values at the index columns).
     */
    public List<RexNode> lowerBound() {
        return lowerBound;
    }

    /**
     * @return Upper index bounds (a row with values at the index columns).
     */
    public List<RexNode> upperBound() {
        return upperBound;
    }

    /** */
    @Override public String toString() {
        return S.toString(IndexConditions.class, this,
            "lower", lowerCond,
            "upper", upperCond,
            "lowerBound", lowerBound,
            "upperBound", upperBound);
    }
}
