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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.junit.Before;
import org.junit.Test;

/**
 * Statistic related simple tests.
 */
public class StatisticsPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    private static final Date MIN_DATE = Date.valueOf("1980-04-09");

    /** */
    private static final Date MAX_DATE = Date.valueOf("2020-04-09");

    /** */
    private static final Time MIN_TIME = Time.valueOf("09:00:00");

    /** */
    private static final Time MAX_TIME = Time.valueOf("21:59:59");

    /** */
    private static final Timestamp MIN_TIMESTAMP = Timestamp.valueOf("1980-04-09 09:00:00");

    /** */
    private static final Timestamp MAX_TIMESTAMP = Timestamp.valueOf("2020-04-09 21:59:59");

    /** */
    private TestTable tbl1;

    /** */
    private TestTable tbl2;

    /** */
    private TestTable tbl3;

    /** */
    private IgniteStatisticsImpl tbl1stat;

    /** */
    private IgniteStatisticsImpl tbl2stat;

    /** */
    private IgniteStatisticsImpl tbl3stat;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        ColocationGroup colocation = ColocationGroup.forAssignments(Arrays.asList(
            select(nodes, 0, 1),
            select(nodes, 1, 2),
            select(nodes, 2, 0),
            select(nodes, 0, 1),
            select(nodes, 1, 2)
        ));

        tbl1 = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("T1C1INT", f.createJavaType(Integer.class))
                .add("T1C2STR", f.createJavaType(String.class))
                .add("T1C3DBL", f.createJavaType(Double.class))
                .add("T1C4BYTE", f.createJavaType(Byte.class))
                .add("T1C5BOOLEAN", f.createJavaType(Boolean.class))
                .add("T1C6CHARACTER", f.createJavaType(Character.class))
                .add("T1C7SHORT", f.createJavaType(Short.class))
                .add("T1C8LONG", f.createJavaType(Long.class))
                .add("T1C9FLOAT", f.createJavaType(Float.class))
                .add("T1C10DATE", f.createJavaType(Date.class))
                .add("T1C11TIME", f.createJavaType(Time.class))
                .add("T1C12TIMESTAMP", f.createJavaType(Timestamp.class))
                .build())
            .setDistribution(IgniteDistributions.affinity(0, "TBL1", "hash"));

        tbl1.addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, tbl1));
        tbl1.addIndex(new IgniteIndex(RelCollations.of(6), "TBL1_SHORT", null, tbl1));
        tbl1.addIndex(new IgniteIndex(RelCollations.of(7), "TBL1_LONG", null, tbl1));
        tbl1.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(6, 7)), "TBL1_SHORT_LONG", null, tbl1));

        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("T1C1INT", new ColumnStatistics(ValueInt.get(1), ValueInt.get(1000),
            0, 1000, 1000, 4, null, 1, 0));

        colStat1.put("T1C2STR", new ColumnStatistics(ValueString.get("A1"), ValueString.get("Z9"),
            100, 20, 1000, 2, null, 1, 0));

        colStat1.put("T1C3DBL", new ColumnStatistics(ValueDouble.get(0.01), ValueDouble.get(0.99),
            0, 1000, 1000, 8, null, 1, 0));

        colStat1.put("T1C4BYTE", new ColumnStatistics(ValueByte.get((byte)0), ValueByte.get((byte)255),
            0, 1000, 1000, 8, null, 1, 0));

        colStat1.put("T1C5BOOLEAN", new ColumnStatistics(ValueBoolean.get(false), ValueBoolean.get(true),
            0, 2, 1000, 1, null, 1, 0));

        colStat1.put("T1C6CHARACTER", new ColumnStatistics(ValueString.get("A"), ValueString.get("Z"),
            10, 10, 1000, 1, null, 1, 0));

        colStat1.put("T1C7SHORT", new ColumnStatistics(ValueShort.get((short)0), ValueShort.get((short)5000),
            110, 500, 1000, 2, null, 1, 0));

        colStat1.put("T1C8LONG", new ColumnStatistics(ValueLong.get(0L), ValueLong.get(100000L),
            0, 100000, 100000, 8, null, 1, 0));

        colStat1.put("T1C9FLOAT", new ColumnStatistics(ValueFloat.get((float)0.1), ValueFloat.get((float)0.9),
            0, 1000, 1000, 8, null, 1, 0));

        colStat1.put("T1C10DATE", new ColumnStatistics(ValueDate.get(MIN_DATE), ValueDate.get(MAX_DATE),
            0, 1000, 1000, 8, null, 1, 0));

        colStat1.put("T1C11TIME", new ColumnStatistics(ValueTime.get(MIN_TIME), ValueTime.get(MAX_TIME),
            0, 1000, 1000, 8, null, 1, 0));

        colStat1.put("T1C12TIMESTAMP", new ColumnStatistics(ValueTimestamp.get(MIN_TIMESTAMP), ValueTimestamp.get(MAX_TIMESTAMP),
            0, 1000, 1000, 8, null, 1, 0));

        tbl1stat = new IgniteStatisticsImpl(new ObjectStatisticsImpl(1000, colStat1));

        tbl2 = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("T2C1INT", f.createJavaType(Integer.class))
                .add("T2C2STR", f.createJavaType(String.class))
                .add("T2C3LONG", f.createJavaType(Long.class))
                .add("T2C4DBL", f.createJavaType(Double.class))
                .build())
            .setDistribution(IgniteDistributions.affinity(0, "TBL2", "hash"));

        tbl2.addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, tbl2));
        tbl2.addIndex(new IgniteIndex(RelCollations.of(2), "TBL2_LONG", null, tbl2));

        HashMap<String, ColumnStatistics> colStat2 = new HashMap<>();
        colStat2.put("T2C1INT", new ColumnStatistics(ValueInt.get(1), ValueInt.get(1000),
            0, 1000, 30000, 4, null, 1, 0));

        tbl2stat = new IgniteStatisticsImpl(new ObjectStatisticsImpl(30000, colStat2));

        tbl3 = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("T3C1INT", f.createJavaType(Integer.class))
                .add("T3C2STR", f.createJavaType(String.class))
                .add("T3C3DBL", f.createJavaType(Double.class))
                .build())
            .setDistribution(IgniteDistributions.affinity(0, "TBL3", "hash"));
        tbl3.addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, tbl3));
    }

    /**
     * Check index usage with and without statistics:
     *
     * 1) With statistics planner choose second one with better selectivity.
     * 2) Without statistics planner choose first one.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testIndexChoosing() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        tbl1.setStatistics(tbl1stat);

        publicSchema.addTable("TBL1", tbl1);
        publicSchema.addTable("TBL2", tbl2);
        publicSchema.addTable("TBL3", tbl3);

        String sql = "select * from TBL1 where t1c7short > 5 and t1c8long > 55555";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNotNull(idxScan);
        assertEquals("TBL1_LONG", idxScan.indexName());

        tbl1.setStatistics(new IgniteStatisticsImpl(null));

        IgniteRel phys2 = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan2 = findFirstNode(phys2, byClass(IgniteIndexScan.class));

        assertNotNull(idxScan2);
        assertEquals("TBL1_SHORT", idxScan2.indexName());

        tbl1.setStatistics(tbl1stat);
    }
}
