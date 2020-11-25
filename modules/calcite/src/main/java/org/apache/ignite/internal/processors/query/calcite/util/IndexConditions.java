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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IndexConditions {
    /** */
    protected List<RexNode> lowerCond;

    /** */
    protected List<RexNode> upperCond;

    /** */
    protected List<RexNode> lowerBound;

    /** */
    protected List<RexNode> upperBound;

    /**
     */
    protected IndexConditions(@Nullable List<RexNode> lowerCond, @Nullable List<RexNode> upperCond) {
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerCondition() {
        return lowerCond;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerBound(RelOptCluster cluster, RelDataType rowType, Mappings.TargetMapping mapping) {
        if (lowerBound == null && lowerCond != null)
            lowerBound = RexUtils.asBound(cluster, lowerCond, rowType, mapping);

        return lowerBound;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperCondition() {
        return upperCond;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperBound(RelOptCluster cluster, RelDataType rowType, Mappings.TargetMapping mapping) {
        if (upperBound == null && upperCond != null)
            upperBound = RexUtils.asBound(cluster, upperCond, rowType, mapping);

        return upperBound;
    }

}
