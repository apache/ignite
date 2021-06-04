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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class AbstractAggregatePlannerTest extends AbstractPlannerTest {
    /**
     * @return REPLICATED test table (ID, VAL0, VAL1, GRP0, GRP1)
     */
    @NotNull protected TestTable createBroadcastTable() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };
        return tbl;
    }

    /**
     * @return PARTITIONED test table (ID, VAL0, VAL1, GRP0, GRP1)
     */
    @NotNull protected AbstractPlannerTest.TestTable createAffinityTable() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "test", "hash");
            }
        };
    }
}
