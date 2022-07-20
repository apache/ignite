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
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.cache.query.index.sorted.keys.BooleanIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ByteIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DoubleIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.FloatIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.LongIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.ShortIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimeIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimestampIndexKey;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
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
    private RelDataType tbl1rt;

    /** */
    private Set<String> tbl1NumericFields = new HashSet<>();

    /** Base table with all types. */
    private TestTable tbl1;

    /** Equal to tbl1 with some complex indexes. */
    private TestTable tbl4;

    /** */
    private IgniteSchema publicSchema;

    /** */
    private IgniteStatisticsImpl tbl1stat;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        int t1rc = 1000;

        tbl1NumericFields.addAll(Arrays.asList("T1C1INT", "T1C3DBL", "T1C4BYTE", "T1C7SHORT", "T1C8LONG", "T1C9FLOAT"));

        tbl1rt = new RelDataTypeFactory.Builder(f)
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
            .build();

        tbl1 = new TestTable(tbl1rt)
            .setDistribution(IgniteDistributions.affinity(0, "TBL1", "hash"));

        tbl1.addIndex("PK", 0);

        for (RelDataTypeField field : tbl1rt.getFieldList()) {
            if (field.getIndex() == 0)
                continue;

            int idx = field.getIndex();
            String name = getIdxName(1, field.getName().toUpperCase());

            tbl1.addIndex(name, idx);
        }

        tbl4 = new TestTable(tbl1rt)
            .setDistribution(IgniteDistributions.affinity(0, "TBL4", "hash"));
        tbl4.addIndex("PK", 0);

        for (RelDataTypeField field : tbl1rt.getFieldList()) {
            if (field.getIndex() == 0)
                continue;

            int idx = field.getIndex();
            String name = getIdxName(4, field.getName().toUpperCase());

            tbl4.addIndex(name, idx);
        }

        tbl4.addIndex("TBL4_SHORT_LONG", 6, 7);

        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("T1C1INT", new ColumnStatistics(new IntegerIndexKey(1), new IntegerIndexKey(1000),
            0, 1000, t1rc, 4, null, 1, 0));

        colStat1.put("T1C2STR", new ColumnStatistics(new StringIndexKey("A1"), new StringIndexKey("Z9"),
            100, 20, t1rc, 2, null, 1, 0));

        colStat1.put("T1C3DBL", new ColumnStatistics(new DoubleIndexKey(0.01), new DoubleIndexKey(0.99),
            10, 1000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C4BYTE", new ColumnStatistics(new ByteIndexKey((byte)0), new ByteIndexKey((byte)255),
            10, 1000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C5BOOLEAN", new ColumnStatistics(new BooleanIndexKey(false), new BooleanIndexKey(true),
            0, 2, t1rc, 1, null, 1, 0));

        colStat1.put("T1C6CHARACTER", new ColumnStatistics(new StringIndexKey("A"), new StringIndexKey("Z"),
            10, 10, t1rc, 1, null, 1, 0));

        colStat1.put("T1C7SHORT", new ColumnStatistics(new ShortIndexKey((short)1), new ShortIndexKey((short)5000),
            110, 500, t1rc, 2, null, 1, 0));

        colStat1.put("T1C8LONG", new ColumnStatistics(new LongIndexKey(1L), new LongIndexKey(100000L),
            10, 100000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C9FLOAT", new ColumnStatistics(new FloatIndexKey((float)0.1), new FloatIndexKey((float)0.9),
            10, 1000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C10DATE", new ColumnStatistics(new DateIndexKey(MIN_DATE), new DateIndexKey(MAX_DATE),
            20, 1000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C11TIME", new ColumnStatistics(new TimeIndexKey(MIN_TIME), new TimeIndexKey(MAX_TIME),
            10, 1000, t1rc, 8, null, 1, 0));

        colStat1.put("T1C12TIMESTAMP", new ColumnStatistics(new TimestampIndexKey(MIN_TIMESTAMP), new TimestampIndexKey(MAX_TIMESTAMP),
            20, 1000, t1rc, 8, null, 1, 0));

        tbl1stat = new IgniteStatisticsImpl(new ObjectStatisticsImpl(1000, colStat1));

        publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TBL1", tbl1);
        publicSchema.addTable("TBL4", tbl4);
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
        tbl1.setStatistics(tbl1stat);

        String sql = "select * from TBL1 where t1c7short > 5 and t1c8long > 55555";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNotNull(idxScan);
        assertEquals("TBL1_T1C8LONG", idxScan.indexName());

        tbl1.setStatistics(new IgniteStatisticsImpl((CacheTableDescriptor)null));

        IgniteRel phys2 = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan2 = findFirstNode(phys2, byClass(IgniteIndexScan.class));

        assertNotNull(idxScan2);
        assertEquals("TBL1_T1C7SHORT", idxScan2.indexName());

        tbl1.setStatistics(tbl1stat);
    }

    /**
     * Check index choosing with is null condition. Due to AbstractIndexScan logic - no index should be choosen.
     */
    @Test
    public void testIsNull() throws Exception {
        tbl1.setStatistics(tbl1stat);

        String isNullTemplate = "select * from TBL1 where %s is null";
        String isNullPKTemplate = "select * from TBL1 where %s is null and T1C1INT is null";

        for (RelDataTypeField field : tbl1rt.getFieldList()) {
            if (field.getIndex() == 0)
                continue;

            if (tbl1NumericFields.contains(field.getName())) {
                String isNullSql = String.format(isNullTemplate, field.getName());
                String isNullPKSql = String.format(isNullPKTemplate, field.getName());

                String idxName = getIdxName(1, field.getName().toUpperCase());

                checkIdxNotUsed(isNullSql, idxName);
                checkIdxNotUsed(isNullPKSql, idxName);
            }
        }
    }

    /**
     * Check index choosing with not null condition. Due to AbstractIndexScan logic - no index should be choosen.
     */
    @Test
    public void testNotNull() throws Exception {
        tbl1.setStatistics(tbl1stat);

        String isNullTemplate = "select * from TBL1 where %s is not null";
        String isNullPKTemplate = "select * from TBL1 where %s is not null and T1C1INT is null";

        for (RelDataTypeField field : tbl1rt.getFieldList()) {
            if (field.getIndex() == 0)
                continue;

            if (tbl1NumericFields.contains(field.getName())) {
                String isNullSql = String.format(isNullTemplate, field.getName());
                String isNullPKSql = String.format(isNullPKTemplate, field.getName());

                String idxName = getIdxName(1, field.getName().toUpperCase());

                checkIdxNotUsed(isNullSql, idxName);
                checkIdxNotUsed(isNullPKSql, idxName);
            }
        }
    }

    /**
     * Test borders with statistics and check that correct index used.
     * @throws Exception In case of errors.
     */
    @Test
    public void testBorders() throws Exception {
        tbl1.setStatistics(tbl1stat);
        String templateFieldIdxLower = "select * from TBL1 where %s > 1000 and T1C1INT > 0";
        String templateFieldIdxLowerOrEq = "select * from TBL1 where %s >= 1000 and T1C1INT > 0";
        String templateFieldIdxUpper = "select * from TBL1 where %s < 1 and T1C1INT > 10";
        String templateFieldIdxUpperOrEq = "select * from TBL1 where %s < 1 and T1C1INT > 10";

        for (RelDataTypeField field : tbl1rt.getFieldList()) {
            if (field.getIndex() == 0)
                continue;

            if (tbl1NumericFields.contains(field.getName())) {
                String sqlLower = String.format(templateFieldIdxLower, field.getName());
                String sqlLowerOrEq = String.format(templateFieldIdxLowerOrEq, field.getName());
                String sqlUpper = String.format(templateFieldIdxUpper, field.getName());
                String sqlUpperOrEq = String.format(templateFieldIdxUpperOrEq, field.getName());

                String idxName = getIdxName(1, field.getName().toUpperCase());

                checkIdxUsed(sqlLower, idxName);
                checkIdxUsed(sqlLowerOrEq, idxName);
                checkIdxUsed(sqlUpper, idxName);
                checkIdxUsed(sqlUpperOrEq, idxName);
            }
        }
        // time
        checkIdxUsed("select * from TBL1 where T1C11TIME < '" + MIN_TIME + "'", "TBL1_T1C11TIME");
        checkIdxUsed("select * from TBL1 where T1C11TIME <= '" + MIN_TIME + "'", "TBL1_T1C11TIME");
        checkIdxUsed("select * from TBL1 where T1C11TIME > '" + MAX_TIME + "'", "TBL1_T1C11TIME");
        checkIdxUsed("select * from TBL1 where T1C11TIME >= '" + MAX_TIME + "'", "TBL1_T1C11TIME");

        // date
        checkIdxUsed("select * from TBL1 where T1C10DATE < '" + MIN_DATE + "'", "TBL1_T1C10DATE");
        checkIdxUsed("select * from TBL1 where T1C10DATE <= '" + MIN_DATE + "'", "TBL1_T1C10DATE");
        checkIdxUsed("select * from TBL1 where T1C10DATE > '" + MAX_DATE + "'", "TBL1_T1C10DATE");
        checkIdxUsed("select * from TBL1 where T1C10DATE >= '" + MAX_DATE + "'", "TBL1_T1C10DATE");

        // timestamp
        checkIdxUsed("select * from TBL1 where T1C12TIMESTAMP < '" + MIN_TIMESTAMP + "'", "TBL1_T1C12TIMESTAMP");
        checkIdxUsed("select * from TBL1 where T1C12TIMESTAMP <= '" + MIN_TIMESTAMP + "'", "TBL1_T1C12TIMESTAMP");
        checkIdxUsed("select * from TBL1 where T1C12TIMESTAMP > '" + MAX_TIMESTAMP + "'", "TBL1_T1C12TIMESTAMP");
        checkIdxUsed("select * from TBL1 where T1C12TIMESTAMP >= '" + MAX_TIMESTAMP + "'", "TBL1_T1C12TIMESTAMP");
    }

    /**
     * Check index usage.
     *
     * @param sql Query.
     * @param idxName Expected index name.
     * @throws Exception In case of errors.
     */
    private void checkIdxUsed(String sql, String idxName) throws Exception {
        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNotNull(idxScan);
        assertEquals(idxName, idxScan.indexName());
    }

    /**
     * Check index is not used.
     *
     * @param sql Query.
     * @param idxName Not expected index name.
     * @throws Exception In case of errors.
     */
    private void checkIdxNotUsed(String sql, String idxName) throws Exception {
        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertTrue(idxScan == null || !idxName.equals(idxScan.indexName()));
    }

    /**
     * Get index name by column.
     *
     * @param tblIdx Table index.
     * @param fieldName Column name.
     * @return Index name.
     */
    private String getIdxName(int tblIdx, String fieldName) {
        return "TBL" + tblIdx + "_" + fieldName;
    }

    /**
     * Run query with expression and check index wouldn't be choosen for the sum of two columns.
     * @throws Exception In case of error.
     */
    @Test
    public void testIndexChoosingFromExpression() throws Exception {
        tbl1.setStatistics(tbl1stat);
        // 1) for sum of two columns
        String sql = "select * from TBL1 where t1c7short + t1c8long > 55555";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNull(idxScan);
    }

    /**
     * Run query with expression and check index wouldn't be choosen for the sum of column with constant.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testIndexChoosingFromSumConst() throws Exception {
        tbl1.setStatistics(tbl1stat);
        String sql = "select * from TBL1 where t1c7short + 1 > 55555";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNull(idxScan);
    }

    /**
     * Run query with expression and check index wouldn't be choosen for the function of column value.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testIndexChoosingFromUnifunction() throws Exception {
        tbl1.setStatistics(tbl1stat);
        String sql = "select * from TBL1 where abs(t1c7short) > 55555";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertNull(idxScan);
    }

    /**
     * Check composite index wouldn't choosen.
     * @throws Exception In case of error.
     */
    @Test
    public void testCompositeIndexAvoid() throws Exception {
        tbl4.setStatistics(tbl1stat);
        checkIdxUsed("select * from TBL4 where t1c7short > 1 and t1c8long > 80000", "TBL4_T1C8LONG");
    }

    /**
     * Check that index over column of type SHORT will be chosen because
     * it has better selectivity: need to scan only last 500 elements
     * whereas index over column of type STRING has default range selectivity
     * equals to 0.5.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testIndexWithBetterSelectivityPreferred() throws Exception {
        int rowCnt = 10_000;

        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("T1C2STR", new ColumnStatistics(new StringIndexKey("A1"), new StringIndexKey("Z9"),
            0, 1, rowCnt, 2, null, 1, 0));

        colStat1.put("T1C7SHORT", new ColumnStatistics(new ShortIndexKey((short)1), new ShortIndexKey((short)5000),
            0, rowCnt, rowCnt, 2, null, 1, 0));

        IgniteStatisticsImpl stat = new IgniteStatisticsImpl(new ObjectStatisticsImpl(1000, colStat1));

        tbl1.setStatistics(stat);

        String sql = "select * from TBL1 where t1c7short > 4500 and T1C2STR > 'asd'";

        IgniteRel phys = physicalPlan(sql, publicSchema);
        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        assertEquals(getIdxName(1, "T1C7SHORT"), idxScan.indexName());
    }
}
