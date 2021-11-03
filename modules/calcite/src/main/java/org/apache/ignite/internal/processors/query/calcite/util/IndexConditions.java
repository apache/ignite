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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.Nullable;

/**
 * Index conditions and bounds holder. Conditions are not printed to terms (serialized). They are used only to calculate selectivity.
 */
public class IndexConditions {
    /**
     *
     */
    private final List<RexNode> lowerCond;

    /**
     *
     */
    private final List<RexNode> upperCond;

    /**
     *
     */
    private final List<RexNode> lowerBound;

    /**
     *
     */
    private final List<RexNode> upperBound;

    /**
     *
     */
    public IndexConditions() {
        this(null, null, null, null);
    }

    /**
     *
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
     *
     */
    public IndexConditions(RelInput input) {
        lowerCond = null;
        upperCond = null;
        lowerBound = input.get("lower") == null ? null : input.getExpressionList("lower");
        upperBound = input.get("upper") == null ? null : input.getExpressionList("upper");
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

    /**
     *
     */
    public Set<Integer> keys() {
        if (upperBound == null && lowerBound == null) {
            return Collections.emptySet();
        }

        Set<Integer> keys = new HashSet<>();

        int cols = lowerBound != null ? lowerBound.size() : upperBound.size();

        for (int i = 0; i < cols; ++i) {
            if (upperBound != null && RexUtils.isNotNull(upperBound.get(i))
                    || lowerBound != null && RexUtils.isNotNull(lowerBound.get(i))) {
                keys.add(i);
            }
        }

        return Collections.unmodifiableSet(keys);
    }

    /**
     * Describes index bounds.
     *
     * @param pw Plan writer
     * @return Plan writer for fluent-explain pattern
     */
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .itemIf("lower", lowerBound, !nullOrEmpty(lowerBound))
                .itemIf("upper", upperBound, !nullOrEmpty(upperBound));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "IndexConditions{"
                + "lowerCond=" + lowerCond
                + ", upperCond=" + upperCond
                + ", lowerBound=" + lowerBound
                + ", upperBound=" + upperBound
                + '}';
    }
}
