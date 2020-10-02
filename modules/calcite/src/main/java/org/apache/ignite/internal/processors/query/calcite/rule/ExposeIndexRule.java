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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class ExposeIndexRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new ExposeIndexRule();

    /** */
    public ExposeIndexRule() {
        super(operandJ(IgniteTableScan.class, null, ExposeIndexRule::preMatch, any()));
    }

    /** */
    private static boolean preMatch(IgniteTableScan scan) {
        return scan.simple() // was not modified by ProjectScanMergeRule or FilterScanMergeRule
            && scan.getTable().unwrap(IgniteTable.class).indexes().size() > 1; // has indexes to expose
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteTableScan scan = call.rel(0);
        RelOptCluster cluster = scan.getCluster();

        RelOptTable optTable = scan.getTable();
        IgniteTable igniteTable = optTable.unwrap(IgniteTable.class);

        List<IgniteIndexScan> indexes = igniteTable.indexes().keySet().stream()
            .map(idxName -> igniteTable.toRel(cluster, optTable, idxName))
            .collect(Collectors.toList());

        assert indexes.size() > 1;

        Map<RelNode, RelNode> equivMap = new HashMap<>(indexes.size() - 1);
        for (int i = 1; i < indexes.size(); i++)
            equivMap.put(indexes.get(i), scan);

        call.transformTo(F.first(indexes), equivMap);
    }
}
