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

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;

/**
 * Rule to convert an {@link IgniteSort} that has {@code offset} or {@code fetch} set to an {@link IgniteLimit} on top
 * of a "pure" {@code Sort} that has no offset or fetch.
 */
public class LimitConverterRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new LimitConverterRule();

    /** */
    LimitConverterRule() {
        super(
            operand(LogicalSort.class, any()),
            "IgniteLimitRule");
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);

        if (sort.offset == null && sort.fetch == null)
            return;

        RelNode input = sort.getInput();

        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy(
                sort.getTraitSet(),
                input,
                sort.getCollation(),
                null,
                null);
        }

        call.transformTo(
            IgniteLimit.create(
                convert(input, input.getTraitSet().replace(IgniteConvention.INSTANCE)),
                sort.offset,
                sort.fetch));
    }
}
