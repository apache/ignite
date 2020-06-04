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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteTableImpl.PK_INDEX_NAME;

/**
 *
 */
public class RegisterIndexRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new RegisterIndexRule();

    /** */
    public RegisterIndexRule() {
        super(operandJ(IgniteTableScan.class, null, scan -> PK_INDEX_NAME.equals(scan.indexName()), any()));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteTableScan rel = call.rel(0);
        RelOptCluster cluster = rel.getCluster();
        RelOptTable table = rel.getTable();
        IgniteTable igniteTable = rel.igniteTable();
        List<IgniteTableScan> indexes = igniteTable.indexes().keySet().stream()
            .filter(idxName -> !PK_INDEX_NAME.equals(idxName))
            .map(idxName -> igniteTable.toRel(cluster, table, idxName))
            .collect(Collectors.toList());

        if (indexes.isEmpty())
            return;

        Map<RelNode, RelNode> equivMap = new HashMap<>();

        for (int i = 1; i < indexes.size(); i++)
            equivMap.put(indexes.get(i), rel);

        call.transformTo(F.first(indexes), equivMap);
    }
}
