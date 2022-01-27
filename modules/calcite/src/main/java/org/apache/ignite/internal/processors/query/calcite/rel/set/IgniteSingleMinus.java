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

package org.apache.ignite.internal.processors.query.calcite.rel.set;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Physical node for MINUS (EXCEPT) operator which inputs satisfy SINGLE distribution.
 */
public class IgniteSingleMinus extends IgniteMinus implements IgniteSingleSetOp {
    /** {@inheritDoc} */
    public IgniteSingleMinus(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    /** */
    public IgniteSingleMinus(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override public IgniteSingleMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new IgniteSingleMinus(getCluster(), traitSet, inputs, all);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSingleMinus(cluster, getTraitSet(), Commons.cast(inputs), all);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public int aggregateFieldsCount() {
        return getInput(0).getRowType().getFieldCount() + COUNTER_FIELDS_CNT;
    }
}
