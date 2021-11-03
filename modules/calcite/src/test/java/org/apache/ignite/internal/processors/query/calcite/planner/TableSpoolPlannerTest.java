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

package org.apache.ignite.internal.processors.query.calcite.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.Test;

/**
 * Table spool test.
 */
public class TableSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void tableSpoolDistributed() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable t0 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build()) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "T0", "hash");
            }
        };

        TestTable t1 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build()) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "T1", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("T0", t0);
        publicSchema.addTable("T1", t1);

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid > t1.jid";

        RelNode phys = physicalPlan(sql, publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeRule");

        assertNotNull(phys);

        IgniteTableSpool tblSpool = findFirstNode(phys, byClass(IgniteTableSpool.class));

        assertNotNull(tblSpool, "Invalid plan:\n" + RelOptUtil.toString(phys));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void tableSpoolBroadcastNotRewindable() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable t0 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                RewindabilityTrait.ONE_WAY) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable t1 = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("JID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build(),
                RewindabilityTrait.ONE_WAY) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("T0", t0);
        publicSchema.addTable("T1", t1);

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        RelNode phys = physicalPlan(sql, publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter",
                "FilterSpoolMergeToHashIndexSpoolRule", "FilterSpoolMergeToSortIndexSpoolRule");

        assertNotNull(phys);

        IgniteTableSpool tblSpool = findFirstNode(phys, byClass(IgniteTableSpool.class));

        assertNotNull(tblSpool, "Invalid plan:\n" + RelOptUtil.toString(phys));
    }
}
