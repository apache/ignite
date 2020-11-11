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
package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public class DbgRule extends RelOptRule {
    /** Instance. */
    public static final DbgRule INSTANCE = new DbgRule();

    /**
     * Constructor.
     */
    private DbgRule() {
        super(operand(Filter.class,
            operand(TableScan.class, none())),
            "DbgRule");
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        System.out.println("+++ " + call.rel(0) + ", " + call.rel(1));
    }
}
