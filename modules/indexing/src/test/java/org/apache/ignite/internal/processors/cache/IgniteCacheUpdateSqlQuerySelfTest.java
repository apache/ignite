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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheUpdateSqlQuerySelfTest extends IgniteCacheAbstractSqlDmlQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite(0).createCache(createAllTypesCacheConfig());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        ignite(0).cache("L2AT").clear();
    }

    /**
     *
     */
    private static CacheConfiguration createAllTypesCacheConfig() {
        CacheConfiguration ccfg = cacheConfig("L2AT", true, true);

        ccfg.setIndexedTypes(Long.class, AllTypes.class);

        return ccfg;
    }

    /**
     *
     */
    @Test
    public void testUpdateSimple() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set p.id = p.id * 2, p.firstName = " +
            "substring(p.firstName, 0, 2) where length(p._key) = ? or p.secondName like ?").setArgs(2, "%ite"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
            leftovers.get(0));

        assertEqualsCollections(asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(asList("k3", createPerson(6, "Sy", "Green"), 6, "Sy", "Green"),
            leftovers.get(3));
    }

    /**
     *
     */
    @Test
    public void testUpdateSingle() {
        boolean oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        try {
            IgniteCache p = cache();

            QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set _val = ? where _key = ?")
                .setArgs(createPerson(2, "Jo", "White"), "FirstKey"));

            c.iterator();

            c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id, _key"));

            List<List<?>> leftovers = c.getAll();

            assertEquals(4, leftovers.size());

            assertEqualsCollections(asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
                leftovers.get(0));

            assertEqualsCollections(asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
                leftovers.get(1));

            assertEqualsCollections(asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
                leftovers.get(2));

            assertEqualsCollections(asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
                leftovers.get(3));
        }
        finally {
            GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);
        }
    }

    /**
     * Test that nested fields could be updated using sql UPDATE just by nested field name.
     */
    @Test
    public void testNestedFieldsUpdate() {
        boolean oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        try {
            IgniteCache<Long, AllTypes> p = ignite(0).cache("L2AT");

            final long ROOT_KEY = 1;

            // Create 1st level value
            AllTypes rootVal = new AllTypes(1L);

            // With random inner field
            rootVal.innerTypeCol = new AllTypes.InnerType(42L);

            p.query(new SqlFieldsQuery(
                "INSERT INTO \"AllTypes\"(_key,_val) VALUES (?, ?)").setArgs(ROOT_KEY, rootVal)
            ).getAll();

            // Update inner fields just by their names
            p.query(new SqlFieldsQuery("UPDATE \"AllTypes\" " +
                "SET \"innerLongCol\" = ?, \"innerStrCol\" = ?, \"arrListCol\" = ?;"
            ).setArgs(50L, "sss", new ArrayList<>(asList(3L, 2L, 1L)))).getAll();

            AllTypes res = p.get(ROOT_KEY);

            AllTypes.InnerType resInner = new AllTypes.InnerType(50L);

            resInner.innerStrCol = "sss";
            resInner.arrListCol = new ArrayList<>(asList(3L, 2L, 1L));

            assertEquals(resInner, res.innerTypeCol);
        }
        finally {
            GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);
        }
    }

    /** */
    @Test
    public void testTypeConversions() throws ParseException {
        boolean oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        try {
            IgniteCache cache = ignite(0).cache("L2AT");

            cache.query(new SqlFieldsQuery("INSERT INTO \"AllTypes\" (_key, _val) VALUES(2, ?)")
                .setArgs(new AllTypes(2L))).getAll();

            cache.query(new SqlFieldsQuery(
                "UPDATE \"AllTypes\" " +
                    "SET " +
                    "\"dateCol\" = '2016-11-30 12:00:00', " +
                    "\"booleanCol\" = false, " +
                    "\"tsCol\" = DATE '2016-12-01' " +
                    "WHERE _key = 2")
            );

            // Look ma, no hands: first we set value of inner object column (innerTypeCol), then update only one of its
            // fields (innerLongCol), while leaving another inner property (innerStrCol) as specified by innerTypeCol.
            cache.query(new SqlFieldsQuery(
                    "UPDATE \"AllTypes\" " +
                        "SET " +
                        "\"innerLongCol\" = ?, " +  // (1)
                        "\"doubleCol\" = CAST('50' as INT), " +
                        "\"booleanCol\" = 80, " +
                        "\"innerTypeCol\" = ?, " + // (2)
                        "\"strCol\" = PI(), " +
                        "\"shortCol\" = CAST(WEEK(PARSEDATETIME('2016-11-30', 'yyyy-MM-dd')) as VARCHAR), " +
                        "\"sqlDateCol\"=TIMESTAMP '2016-12-02 13:47:00', " +
                        "\"tsCol\"=TIMESTAMPADD('MI', 2, DATEADD('DAY', 2, \"tsCol\")), " +
                        "\"primitiveIntsCol\" = ?, " +  //(3)
                        "\"bytesCol\" = ?" // (4)
                ).setArgs(5, new AllTypes.InnerType(80L), new int[] {2, 3}, new Byte[] {4, 5, 6})
            ).getAll();

            AllTypes res = (AllTypes) cache.get(2L);

            assertNotNull(res);

            assertEquals(new BigDecimal(301.0).doubleValue(), res.bigDecimalCol.doubleValue());
            assertEquals(50.0, res.doubleCol);
            assertEquals(2L, (long) res.longCol);
            assertTrue(res.booleanCol);
            assertEquals("3.141592653589793", res.strCol);
            assertTrue(Arrays.equals(new byte[] {0, 1}, res.primitiveBytesCol));
            assertTrue(Arrays.equals(new Byte[] {4, 5, 6}, res.bytesCol));
            assertTrue(Arrays.deepEquals(new Integer[] {0, 1}, res.intsCol));
            assertTrue(Arrays.equals(new int[] {2, 3}, res.primitiveIntsCol));

            AllTypes.InnerType expInnerType = new AllTypes.InnerType(80L);
            expInnerType.innerLongCol = 5L;

            assertEquals(expInnerType, res.innerTypeCol);
            assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse("2016-11-30 12:00:00"), res.dateCol);
            assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse("2016-12-03 00:02:00"), res.tsCol);
            assertEquals(2, res.intCol);
            assertEquals(AllTypes.EnumType.ENUMTRUE, res.enumCol);
            assertEquals(new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse("2016-12-02").getTime()), res.sqlDateCol);

            // 49th week, right?
            assertEquals(49, res.shortCol);
        }
        finally {
            GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);
        }
    }

    /** */
    @Test
    public void testSingleInnerFieldUpdate() throws ParseException {
        boolean oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        try {
            IgniteCache cache = ignite(0).cache("L2AT");

            cache.query(new SqlFieldsQuery("insert into \"AllTypes\" (_key, _val) values(2, ?)")
                .setArgs(new AllTypes(2L))).getAll();

            cache.query(new SqlFieldsQuery(
                "UPDATE \"AllTypes\" " +
                    "SET " +
                    "\"dateCol\" = '2016-11-30 12:00:00', " +
                    "\"booleanCol\" = false " +
                    "WHERE _key = 2")
            ).getAll();

            assertFalse(cache.query(new SqlFieldsQuery("select * from \"AllTypes\"")).getAll().isEmpty());

            cache.query(new SqlFieldsQuery("update \"AllTypes\" set \"innerLongCol\" = 5"));

            AllTypes res = (AllTypes) cache.get(2L);

            assertNotNull(res);

            assertEquals(new BigDecimal(301.0).doubleValue(), res.bigDecimalCol.doubleValue());
            assertEquals(3.01, res.doubleCol);
            assertEquals(2L, (long) res.longCol);
            assertFalse(res.booleanCol);

            assertEquals("2", res.strCol);
            assertTrue(Arrays.equals(new byte[] {0, 1}, res.primitiveBytesCol));
            assertTrue(Arrays.deepEquals(new Byte[] {0, 1}, res.bytesCol));
            assertTrue(Arrays.deepEquals(new Integer[] {0, 1}, res.intsCol));
            assertTrue(Arrays.equals(new int[] {0, 1}, res.primitiveIntsCol));

            AllTypes.InnerType expInnerType = new AllTypes.InnerType(2L);
            expInnerType.innerLongCol = 5L;

            assertEquals(expInnerType, res.innerTypeCol);
            assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse("2016-11-30 12:00:00"), res.dateCol);
            assertNull(res.tsCol);
            assertEquals(2, res.intCol);
            assertEquals(AllTypes.EnumType.ENUMTRUE, res.enumCol);
            assertNull(res.sqlDateCol);

            assertEquals(-23000, res.shortCol);
        }
        finally {
            GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);
        }
    }

    /**
     * Drop and create tables for the update parameters tests.
     */
    private void dropAndCreateParamsTables() {
        execute(new SqlFieldsQuery("DROP TABLE IF EXISTS PARAMS"));
        execute(new SqlFieldsQuery("DROP TABLE IF EXISTS CONST_TAB"));

        execute(new SqlFieldsQuery("CREATE TABLE PARAMS(ID INT PRIMARY KEY, VAL1 INT, VAL2 INT) WITH \"template=replicated\""));
        execute(new SqlFieldsQuery("INSERT INTO PARAMS VALUES (1, 2, 3), (2, 3, 4), (3, 2, 1), (4, 2, 1)"));

        execute(new SqlFieldsQuery("CREATE TABLE CONST_TAB(ID INT PRIMARY KEY, VAL INT) WITH \"template=replicated\""));
        execute(new SqlFieldsQuery("INSERT INTO CONST_TAB VALUES (42, 2)"));
    }

    /**
     * Check parameters from update are passed to implicit select correctly.
     */
    @Test
    public void testUpdateParameters() {
        asList(true, false).forEach(loc -> {
            dropAndCreateParamsTables();

            execute(new SqlFieldsQuery(
                "UPDATE PARAMS SET VAL1 = ?, VAL2 = ? WHERE ID = ?")
                .setArgs(1, 2, 3).setLocal(loc));

            List<List<?>> tab = execute(new SqlFieldsQuery("SELECT * FROM PARAMS ORDER BY ID"));

            List<List<?>> exp = asList(asList(1, 2, 3), asList(2, 3, 4), asList(3, 1, 2), asList(4, 2, 1));

            assertEqualsCollections(exp, tab);
        });
    }

    /**
     * Check query positional parameters using update statement. Parameters have explicit indexes.
     */
    @Test
    public void testUpdateParametersWithIndexes() {
        asList(true, false).forEach(loc -> {
            dropAndCreateParamsTables();

            execute(new SqlFieldsQuery(
                "UPDATE PARAMS SET VAL1 = ?3, VAL2 = ?2 WHERE ID = ?1")
                .setArgs(1, 2, 3).setLocal(loc));

            List<List<?>> tab = execute(new SqlFieldsQuery("SELECT * FROM PARAMS ORDER BY ID"));

            List<List<?>> exp = asList(asList(1, 3, 2), asList(2, 3, 4), asList(3, 2, 1), asList(4, 2, 1));

            assertEqualsCollections(exp, tab);
        });
    }

    /**
     * Check query positional parameters using update statement with the subquery.
     */
    @Test
    public void testUpdateParametersWithSubQuery() {
        asList(true, false).forEach(loc -> {
            dropAndCreateParamsTables();

            execute(new SqlFieldsQuery(
                "UPDATE PARAMS SET VAL1 = ?, VAL2 = ( SELECT ID FROM CONST_TAB WHERE VAL = ? ) WHERE ID = ?")
                .setArgs(1, 2, 3).setLocal(loc));

            List<List<?>> tab = execute(new SqlFieldsQuery("SELECT * FROM PARAMS ORDER BY ID"));

            List<List<?>> exp = asList(asList(1, 2, 3), asList(2, 3, 4), asList(3, 1, 42), asList(4, 2, 1));

            assertEqualsCollections(exp, tab);
        });
    }

    /**
     * Execute sql query with the public schema.
     *
     * @param qry to execute.
     * @return fetched result.
     */
    private List<List<?>> execute(SqlFieldsQuery qry) {
        return ignite(0).cache("L2AT").query(qry.setSchema("PUBLIC")).getAll();
    }

    /**
     *
     */
    static final class AllTypes implements Serializable {
        /**
         * Data Long.
         */
        @QuerySqlField
        Long longCol;

        /**
         * Data double.
         */
        @QuerySqlField
        double doubleCol;

        /**
         * Data String.
         */
        @QuerySqlField
        String strCol;

        /**
         * Data boolean.
         */
        @QuerySqlField
        boolean booleanCol;

        /**
         * Date.
         */
        @QuerySqlField
        Date dateCol;

        /**
         * SQL date (non timestamp).
         */
        @QuerySqlField
        java.sql.Date sqlDateCol;

        /**
         * Timestamp.
         */
        @QuerySqlField
        Timestamp tsCol;

        /**
         * Data int.
         */
        @QuerySqlField
        int intCol;

        /**
         * BigDecimal
         */
        @QuerySqlField
        BigDecimal bigDecimalCol;

        /**
         * Data bytes array.
         */
        @QuerySqlField
        Byte[] bytesCol;

        /**
         * Data bytes primitive array.
         */
        @QuerySqlField
        byte[] primitiveBytesCol;

        /**
         * Data bytes array.
         */
        @QuerySqlField
        Integer[] intsCol;

        /**
         * Data bytes primitive array.
         */
        @QuerySqlField
        int[] primitiveIntsCol;

        /**
         * Data bytes array.
         */
        @QuerySqlField
        short shortCol;

        /**
         * Inner type object.
         */
        @QuerySqlField
        InnerType innerTypeCol;

        /** */
        static class InnerType implements Serializable {
            /** */
            @QuerySqlField
            Long innerLongCol;

            /** */
            @QuerySqlField
            String innerStrCol;

            /** */
            @QuerySqlField
            ArrayList<Long> arrListCol = new ArrayList<>();

            /** */
            InnerType(Long key) {
                innerLongCol = key;
                innerStrCol = Long.toString(key);

                Long m = key % 8;

                for (Integer i = 0; i < m; i++)
                    arrListCol.add(key + i);
            }

            /**
             * {@inheritDoc}
             */
            @Override public String toString() {
                return "[Long=" + Long.toString(innerLongCol) +
                    ", String='" + innerStrCol + "'" +
                    ", ArrayList=" + arrListCol.toString() +
                    "]";
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                InnerType innerType = (InnerType) o;

                if (innerLongCol != null ? !innerLongCol.equals(innerType.innerLongCol) : innerType.innerLongCol != null)
                    return false;
                if (innerStrCol != null ? !innerStrCol.equals(innerType.innerStrCol) : innerType.innerStrCol != null)
                    return false;
                return arrListCol != null ? arrListCol.equals(innerType.arrListCol) : innerType.arrListCol == null;

            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                int res = innerLongCol != null ? innerLongCol.hashCode() : 0;
                res = 31 * res + (innerStrCol != null ? innerStrCol.hashCode() : 0);
                res = 31 * res + (arrListCol != null ? arrListCol.hashCode() : 0);
                return res;
            }
        }

        /** */
        @QuerySqlField
        EnumType enumCol;

        /** */
        enum EnumType {
            /** */
            ENUMTRUE,

            /** */
            ENUMFALSE
        }

        /** */
        private void init(Long key, String str) {
            this.longCol = key;
            this.doubleCol = Math.round(1000 * Math.log10(longCol.doubleValue()));
            this.bigDecimalCol = BigDecimal.valueOf(doubleCol);
            this.doubleCol = doubleCol / 100;
            this.strCol = str;
            if (key % 2 == 0) {
                this.booleanCol = true;
                this.enumCol = EnumType.ENUMTRUE;
                this.innerTypeCol = new InnerType(key);
            }
            else {
                this.booleanCol = false;
                this.enumCol = EnumType.ENUMFALSE;
                this.innerTypeCol = null;
            }
            this.intCol = key.intValue();
            this.bytesCol = new Byte[(int) (key % 10)];
            this.intsCol = new Integer[(int) (key % 10)];
            this.primitiveBytesCol = new byte[(int) (key % 10)];
            this.primitiveIntsCol = new int[(int) (key % 10)];
            //this.bytesCol = new Byte[10];
            int b = 0;
            for (int j = 0; j < bytesCol.length; j++) {
                if (b == 256)
                    b = 0;
                bytesCol[j] = (byte) b;
                primitiveBytesCol[j] = (byte) b;
                intsCol[j] = b;
                primitiveIntsCol[j] = b;
                b++;
            }
            this.shortCol = (short) (((1000 * key) % 50000) - 25000);

            dateCol = new Date();
        }

        /** */
        AllTypes(Long key) {
            this.init(key, Long.toString(key));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AllTypes allTypes = (AllTypes) o;

            if (Double.compare(allTypes.doubleCol, doubleCol) != 0) return false;
            if (booleanCol != allTypes.booleanCol) return false;
            if (intCol != allTypes.intCol) return false;
            if (shortCol != allTypes.shortCol) return false;
            if (longCol != null ? !longCol.equals(allTypes.longCol) : allTypes.longCol != null) return false;
            if (strCol != null ? !strCol.equals(allTypes.strCol) : allTypes.strCol != null) return false;
            if (dateCol != null ? !dateCol.equals(allTypes.dateCol) : allTypes.dateCol != null) return false;
            if (sqlDateCol != null ? !sqlDateCol.equals(allTypes.sqlDateCol) : allTypes.sqlDateCol != null) return false;
            if (tsCol != null ? !tsCol.equals(allTypes.tsCol) : allTypes.tsCol != null) return false;
            if (bigDecimalCol != null ? !bigDecimalCol.equals(allTypes.bigDecimalCol) : allTypes.bigDecimalCol != null)
                return false;
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(bytesCol, allTypes.bytesCol)) return false;
            if (innerTypeCol != null ? !innerTypeCol.equals(allTypes.innerTypeCol) : allTypes.innerTypeCol != null)
                return false;
            return enumCol == allTypes.enumCol;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res;
            long temp;
            res = longCol != null ? longCol.hashCode() : 0;
            temp = Double.doubleToLongBits(doubleCol);
            res = 31 * res + (int) (temp ^ (temp >>> 32));
            res = 31 * res + (strCol != null ? strCol.hashCode() : 0);
            res = 31 * res + (booleanCol ? 1 : 0);
            res = 31 * res + (dateCol != null ? dateCol.hashCode() : 0);
            res = 31 * res + (sqlDateCol != null ? sqlDateCol.hashCode() : 0);
            res = 31 * res + (tsCol != null ? tsCol.hashCode() : 0);
            res = 31 * res + intCol;
            res = 31 * res + (bigDecimalCol != null ? bigDecimalCol.hashCode() : 0);
            res = 31 * res + Arrays.hashCode(bytesCol);
            res = 31 * res + (int) shortCol;
            res = 31 * res + (innerTypeCol != null ? innerTypeCol.hashCode() : 0);
            res = 31 * res + (enumCol != null ? enumCol.hashCode() : 0);
            return res;
        }
    }
}
