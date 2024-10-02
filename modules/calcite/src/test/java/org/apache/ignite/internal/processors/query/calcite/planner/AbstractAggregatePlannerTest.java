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

import java.util.Arrays;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
@SuppressWarnings({"TypeMayBeWeakened"})
public class AbstractAggregatePlannerTest extends AbstractPlannerTest {
    /**
     * @return REPLICATED test table (ID, VAL0, VAL1, GRP0, GRP1) with given defined distribution.
     */
    @NotNull protected TestTable createTable(IgniteDistribution distr) {
        return new TestTable(createType()) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }
        };
    }

    /**
     * @return REPLICATED test table (ID, VAL0, VAL1, GRP0, GRP1)
     */
    @NotNull protected TestTable createBroadcastTable() {
        return createTable(IgniteDistributions.broadcast());
    }

    /**
     * @return PARTITIONED test table (ID, VAL0, VAL1, GRP0, GRP1)
     */
    @NotNull protected TestTable createAffinityTable() {
        return new TestTable(createType()) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "test", "hash");
            }
        };
    }

    /**
     * @return Test table rel type.
     */
    private static RelDataType createType() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return new RelDataTypeFactory.Builder(f)
            .add("ID", f.createJavaType(Integer.class))
            .add("VAL0", f.createJavaType(Integer.class))
            .add("VAL1", f.createJavaType(Integer.class))
            .add("GRP0", f.createJavaType(Integer.class))
            .add("GRP1", f.createJavaType(Integer.class))
            .build();
    }
}
