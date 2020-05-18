/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Basic index tests.
 */
public class CalciteBasicSecondaryIndexIntegrationTest extends GridCommonAbstractTest {
    private static final String PK = IgniteTable.PK_INDEX_NAME;
    private static final String PK_ALIAS = IgniteTable.PK_ALIAS_INDEX_NAME;
    private static final String DEPID_IDX = "DEPID_IDX";
    private static final String NAME_CITY_IDX = "NAME_CITY_IDX";
    private static final String NAME_DEPID_CITY_IDX = "NAME_DEPID_CITY_IDX";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(2);

        QueryEntity projEntity = new QueryEntity();
        projEntity.setKeyType(Integer.class.getName());
        projEntity.setKeyFieldName("id");
        projEntity.setValueType(Developer.class.getName());
        projEntity.addQueryField("id", Integer.class.getName(), null);
        projEntity.addQueryField("name", String.class.getName(), null);
        projEntity.addQueryField("depId", Integer.class.getName(), null);
        projEntity.addQueryField("city", String.class.getName(), null);
        projEntity.addQueryField("age", Integer.class.getName(), null);

        QueryIndex simpleIdx = new QueryIndex("depId", true);
        simpleIdx.setName(DEPID_IDX);

        LinkedHashMap<String, Boolean> fields1 = new LinkedHashMap<>();
        fields1.put("name", false);
        fields1.put("city", false);
        QueryIndex complexIdxNameId = new QueryIndex(fields1, QueryIndexType.SORTED);
        complexIdxNameId.setName(NAME_CITY_IDX);

        LinkedHashMap<String, Boolean> fields2 = new LinkedHashMap<>();
        fields2.put("name", true);
        fields2.put("depId", false);
        fields2.put("city", false);
        QueryIndex complexIdxNameVer = new QueryIndex(fields2, QueryIndexType.SORTED);
        complexIdxNameVer.setName(NAME_DEPID_CITY_IDX);

        projEntity.setIndexes(asList(simpleIdx, complexIdxNameId, complexIdxNameVer));
        projEntity.setTableName("Developer");

        CacheConfiguration<Integer, Developer> projCfg = cache(projEntity);

        IgniteCache<Integer, Developer> devCache = grid.createCache(projCfg);

        devCache.put(1, new Developer("Mozart", 3, "Vienna", 33));
        devCache.put(2, new Developer("Beethoven", 2, "Vienna", 44));
        devCache.put(3, new Developer("Bach", 1, "Leipzig", 55));
        devCache.put(4, new Developer("Strauss", 2, "Munich", 66));

        awaitPartitionMapExchange();
    }

    /** */
    private CacheConfiguration cache(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    // ===== No filter =====

    /** */
    @Test
    public void testNoFilter() {
        assertQuery("SELECT * FROM Developer")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    // ===== _key filter =====

    /** */
    @Test
    public void testKeyColumnEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key=1")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key>3")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key>=?")
            .withParams(3)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key<?")
            .withParams(3)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key<=2")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    // ===== alias filter =====

    /** */
    @Test
    public void testKeyAliasEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id=2")
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>?")
            .withParams(3)
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>=3")
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<3")
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<=2")
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    // ===== indexed field filter =====

    /** */
    @Test
    public void testIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId=2")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>2")
            .withParams(3)
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>=2")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<?")
            .withParams(3)
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<=?")
            .withParams(2)
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    // ===== non-indexed field filter =====

    /** */
    @Test
    public void testNonIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age=?")
            .withParams(44)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>?")
            .withParams(50)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>=?")
            .withParams(34)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<?")
            .withParams(56)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<=?")
            .withParams(55)
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Test
    public void testComplexIndexCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3")
            .containsScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition2() {
        assertQuery("SELECT * FROM Developer WHERE depId=? AND name=?")
            .withParams(3, "Mozart")
            .containsScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition4() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Leipzig'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition5() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND city='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition6() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition7() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition8() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2 AND age>20")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition9() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId>=2 AND city>='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition10() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND city>='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition11() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition12() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition13() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=3 AND city='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition14() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition15() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Vienna'")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition16() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND (city='Vienna' AND depId=3)")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testEmptyResult() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Leipzig'")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .check();
    }

    /** */
    @Test
    public void testOrCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' OR depId=1")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    public void testOrCondition2() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (depId=1 OR depId=3)")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testOrCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (age > 22 AND (depId=1 OR depId=3))")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Ignore("TODO")
    @Test
    public void testOrderByKey() {
        assertQuery("SELECT id, name, depId, age FROM Developer ORDER BY _key")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .doesNotContainSubPlan("IgniteSort")
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByKeyAlias() {
        assertQuery("SELECT * FROM Developer ORDER BY id")
            .containsScan("PUBLIC", "DEVELOPER", PK_ALIAS)
            .doesNotContainSubPlan("IgniteSort")
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByDepId() {
        assertQuery("SELECT * FROM Developer ORDER BY depId")
            .containsScan("PUBLIC", "DEVELOPER", DEPID_IDX)
            .doesNotContainSubPlan("IgniteSort")
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityAsc() {
        assertQuery("SELECT * FROM Developer ORDER BY name, city")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .containsSubPlan("IgniteSort")
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(4, "Strauss", 2, "Munich", 66)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityDesc() {
        assertQuery("SELECT * FROM Developer ORDER BY name DESC, city DESC")
            .containsScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX)
            .doesNotContainSubPlan("IgniteSort")
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNoIndexedColumn() {
        assertQuery("SELECT * FROM Developer ORDER BY age DESC")
            .containsScan("PUBLIC", "DEVELOPER", PK)
            .containsSubPlan("IgniteSort")
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .ordered()
            .check();
    }

    /** */
    private QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry);
    }

    /** */
    private class QueryChecker {
        /** */
        private String qry;

        /** */
        private List<String> subPlans = new ArrayList<>();

        /** */
        private List<String> excludedSubPlans = new ArrayList<>();

        /** */
        private boolean ordered;

        /** */
        private List<List<?>> expectedResult = new ArrayList<>();

        /** */
        private Object[] params = X.EMPTY_OBJECT_ARRAY;

        /** */
        private String exactPlan;

        /** */
        public QueryChecker(String qry) {
            this.qry = qry;
        }

        /** */
        public QueryChecker containsSubPlan(String subPlan) {
            subPlans.add(subPlan);

            return this;
        }

        /** */
        public QueryChecker doesNotContainSubPlan(String subPlan) {
            excludedSubPlans.add(subPlan);

            return this;
        }

        /** */
        public QueryChecker ordered() {
            ordered = true;

            return this;
        }

        /** */
        public QueryChecker withParams(Object... params) {
            this.params = params;

            return this;
        }

        /** */
        public QueryChecker returns(Object... res) {
            expectedResult.add(Arrays.asList(res));

            return this;
        }

        /** */
        public QueryChecker containsScan(String schema, String tblName, String idxName) {
            String idxScanName = "IgniteTableScan(table=[[" + schema + ", " + tblName + "]], index=[" + idxName + ']';

            return containsSubPlan(idxScanName);
        }

        /** */
        public QueryChecker planEquals(String plan) {
            exactPlan = plan;

            return this;
        }

        /** */
        public void check() {
            // Check plan.
            QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

            List<FieldsQueryCursor<List<?>>> explainCursors =
                engine.query(null, "PUBLIC", "EXPLAIN PLAN FOR " + qry);

            FieldsQueryCursor<List<?>> explainCursor = explainCursors.get(0);
            List<List<?>> explainRes = explainCursor.getAll();
            String actualPlan = (String)explainRes.get(0).get(0);

            for (String subPlan : subPlans) {
                assertTrue("\nExpected subPlan:\n" + subPlan + "\nactual plan:\n" + actualPlan,
                    actualPlan.contains(subPlan));
            }

            for (String subPlan : excludedSubPlans) {
                assertTrue("\nExpected plan should not contain:\n" + subPlan + "\nactual plan:\n" + actualPlan,
                    !actualPlan.contains(subPlan));
            }

            if (exactPlan != null) {
                assertEquals(exactPlan, actualPlan);
            }

            // Check result.
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC", qry, params);

            FieldsQueryCursor<List<?>> cur = cursors.get(0);

            List<List<?>> res = cur.getAll();

            if (!ordered) {
                // Avoid arbitrary order.
                res.sort(new ListComparator());
                expectedResult.sort(new ListComparator());
            }

            assertEqualsCollections(expectedResult, res);
        }
    }

    /** */
    private static class ListComparator implements Comparator<List<?>> {
        /** {@inheritDoc} */
        @Override public int compare(List<?> o1, List<?> o2) {
            if (o1.size() != o2.size())
                fail("Collections are not equal:\nExpected:\t" + o1 + "\nActual:\t" + o2);

            Iterator<?> it1 = o1.iterator();
            Iterator<?> it2 = o2.iterator();

            while (it1.hasNext()) {
                Object item1 = it1.next();
                Object item2 = it2.next();

                if (F.eq(item1, item2))
                    continue;

                if (item1 == null)
                    return 1;

                if (item2 == null)
                    return -1;

                if (!(item1 instanceof Comparable) && !(item2 instanceof Comparable))
                    continue;

                Comparable c1 = (Comparable)item1;
                Comparable c2 = (Comparable)item2;

                int c = c1.compareTo(c2);

                if (c != 0)
                    return c;
            }

            return 0;
        }
    }

    /** */
    private static class Developer {
        /** */
        String name;

        /** */
        int depId;

        /** */
        String city;

        /** */
        int age;

        /** */
        public Developer(String name, int depId, String city, int age) {
            this.name = name;
            this.depId = depId;
            this.city = city;
            this.age = age;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Project{" +
                "name='" + name + '\'' +
                ", ver=" + depId +
                '}';
        }
    }
}
