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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteTableImpl.PK_INDEX_NAME;

/**
 *
 */
public class ExposeIndexRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new ExposeIndexRule();

    /** */
    public ExposeIndexRule() {
        super(operandJ(IgniteIndexScan.class, null, ExposeIndexRule::preMatch, any()));
    }

    /** */
    private static boolean preMatch(IgniteIndexScan scan) {
        return scan.igniteTable().indexes().size() > 1     // has indexes to expose
            && PK_INDEX_NAME.equals(scan.indexName())      // is PK index scan
            && scan.condition() == null;                   // was not modified by PushFilterIntoScanRule
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteIndexScan scan = call.rel(0);
        RelOptCluster cluster = scan.getCluster();

        RelOptTable optTable = scan.getTable();
        IgniteTable igniteTable = scan.igniteTable();

        assert PK_INDEX_NAME.equals(scan.indexName());

        Set<String> indexNames = igniteTable.indexes().keySet();

        assert indexNames.size() > 1;

        List<IgniteIndexScan> indexes = new ArrayList<>();
        for (String idxName : indexNames) {
            if (PK_INDEX_NAME.equals(idxName))
                continue;

            indexes.add(igniteTable.toRel(cluster, optTable, idxName));
        }

        Map<RelNode, RelNode> equivMap = new HashMap<>();
        for (int i = 1; i < indexes.size(); i++)
            equivMap.put(indexes.get(i), scan);

        call.transformTo(F.first(indexes), equivMap);
    }
}
