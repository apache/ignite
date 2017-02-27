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
    public void testUpdateSimple() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set p.id = p.id * 2, p.firstName = " +
            "substring(p.firstName, 0, 2) where length(p._key) = ? or p.secondName like ?").setArgs(2, "%ite"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(6, "Sy", "Green"), 6, "Sy", "Green"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testUpdateSingle() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set _val = ? where _key = ?")
            .setArgs(createPerson(2, "Jo", "White"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by id, _key"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testUpdateValueAndFields() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set id = ?, _val = ? where _key = ?")
            .setArgs(44, createPerson(2, "Jo", "Woo"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(44, "Jo", "Woo"), 44, "Jo", "Woo"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testDefault() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set id = DEFAULT, _val = ? where _key = ?")
            .setArgs(createPerson(2, "Jo", "Woo"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(0, "Jo", "Woo"), 0, "Jo", "Woo"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(3));
    }

    /** */
    public void testTypeConversions() throws ParseException {
        IgniteCache cache = ignite(0).cache("L2AT");

        cache.query(new SqlFieldsQuery("insert into \"AllTypes\"(_key, _val, \"dateCol\", \"booleanCol\"," +
            "\"tsCol\") values(2, ?, '2016-11-30 12:00:00', false, DATE '2016-12-01')").setArgs(new AllTypes(2L)));

        cache.query(new SqlFieldsQuery("select \"primitiveIntsCol\" from \"AllTypes\"")).getAll();

        cache.query(new SqlFieldsQuery("update \"AllTypes\" set \"doubleCol\" = CAST('50' as INT)," +
            " \"booleanCol\" = 80, \"innerTypeCol\" = ?, \"strCol\" = PI(), \"shortCol\" = " +
            "CAST(WEEK(PARSEDATETIME('2016-11-30', 'yyyy-MM-dd')) as VARCHAR), " +
            "\"sqlDateCol\"=TIMESTAMP '2016-12-02 13:47:00', \"tsCol\"=TIMESTAMPADD('MI', 2, " +
            "DATEADD('DAY', 2, \"tsCol\")), \"primitiveIntsCol\" = ?, \"bytesCol\" = ?")
            .setArgs(new AllTypes.InnerType(80L), new int[] {2, 3}, new Byte[] {4, 5, 6}));

        AllTypes res = (AllTypes) cache.get(2L);

        assertEquals(new BigDecimal(301.0).doubleValue(), res.bigDecimalCol.doubleValue());
        assertEquals(50.0, res.doubleCol);
        assertEquals(2L, (long) res.longCol);
        assertTrue(res.booleanCol);
        assertEquals("3.141592653589793", res.strCol);
        assertTrue(Arrays.equals(new byte[] {0, 1}, res.primitiveBytesCol));
        assertTrue(Arrays.equals(new Byte[] {4, 5, 6}, res.bytesCol));
        assertTrue(Arrays.deepEquals(new Integer[] {0, 1}, res.intsCol));
        assertTrue(Arrays.equals(new int[] {2, 3}, res.primitiveIntsCol));
        assertEquals(new AllTypes.InnerType(80L), res.innerTypeCol);
        assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse("2016-11-30 12:00:00"), res.dateCol);
        assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse("2016-12-03 00:02:00"), res.tsCol);
        assertEquals(2, res.intCol);
        assertEquals(AllTypes.EnumType.ENUMTRUE, res.enumCol);
        assertEquals(new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse("2016-12-02").getTime()), res.sqlDateCol);

        // 49th week, right?
        assertEquals(49, res.shortCol);
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
        static final class InnerType implements Serializable {
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
