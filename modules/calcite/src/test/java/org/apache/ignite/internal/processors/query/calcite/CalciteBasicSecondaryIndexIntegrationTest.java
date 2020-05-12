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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Basic index tests.
 */
public class CalciteBasicSecondaryIndexIntegrationTest extends GridCommonAbstractTest {
    private static final String PK = IgniteTable.PK_INDEX_NAME;
    private static final String PK_ALIAS = IgniteTable.PK_ALIAS_INDEX_NAME;
    private static final String DEPID_IDX =  "DEPID_IDX";
    private static final String NAME_CITY_IDX =  "NAME_CITY_IDX";
    private static final String NAME_DEPID_CITY_IDX =  "NAME_DEPID_CITY_IDX";

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
        checkQuery("SELECT * FROM Developer",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66))
        );
    }

    // ===== _key filter =====

    /** */
    @Test
    public void testKeyColumnEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE _key=1",
            "PUBLIC, DEVELOPER", PK, false,
            singletonList(asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE _key>3",
            "PUBLIC, DEVELOPER", PK, false,
            singletonList(asList(4, "Strauss", 2, "Munich", 66))
        );
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE _key>=?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66)),
        3
        );
    }

    /** */
    @Test
    public void testKeyColumnLessThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE _key<?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44)),
            3
        );
    }

    /** */
    @Test
    public void testKeyColumnLessThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE _key<=2",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44))
        );
    }

    // ===== alias filter =====

    /** */
    @Test
    public void testKeyAliasEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE id=2",
            "PUBLIC, DEVELOPER", PK_ALIAS, false,
            singletonList(asList(2, "Beethoven", 2, "Vienna", 44))
        );
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE id>?",
            "PUBLIC, DEVELOPER", PK_ALIAS, false,
            singletonList(asList(4, "Strauss", 2, "Munich", 66)),
            3
        );
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE id>=3",
            "PUBLIC, DEVELOPER", PK_ALIAS, false,
            asList(
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66))
        );
    }

    /** */
    @Test
    public void testKeyAliasLessThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE id<3",
            "PUBLIC, DEVELOPER", PK_ALIAS, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44))
        );
    }

    /** */
    @Test
    public void testKeyAliasLessThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE id<=2",
            "PUBLIC, DEVELOPER", PK_ALIAS, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44))
        );
    }

    // ===== indexed field filter =====

    /** */
    @Test
    public void testIndexedFieldEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE depId=2",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(4, "Strauss", 2, "Munich", 66))
        );
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE depId>2",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            singletonList(asList(1, "Mozart", 3, "Vienna", 33)),
            3
        );
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE depId>=2",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(4, "Strauss", 2, "Munich", 66))
        );
    }

    /** */
    @Test
    public void testIndexedFieldLessThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE depId<?",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66)),
            3
        );
    }

    /** */
    @Test
    public void testIndexedFieldLessThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE depId<=?",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66)),
            2
        );
    }

    // ===== non-indexed field filter =====

    /** */
    @Test
    public void testNonIndexedFieldEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE age=?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(2, "Beethoven", 2, "Vienna", 44)),
            44
        );
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE age>?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66)),
            50
        );
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE age>=?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55),
                asList(4, "Strauss", 2, "Munich", 66)),
            34
        );
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanFilter() {
        checkQuery("SELECT * FROM Developer WHERE age<?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55)),
            56
        );
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanOrEqualsFilter() {
        checkQuery("SELECT * FROM Developer WHERE age<=?",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(2, "Beethoven", 2, "Vienna", 44),
                asList(3, "Bach", 1, "Leipzig", 55)),
            55
        );
    }

    // ===== non-indexed field filter =====

    /** */
    @Test
    public void testComplexIndexCondition1() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3",
            "PUBLIC, DEVELOPER", NAME_DEPID_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition2() {
        checkQuery("SELECT * FROM Developer WHERE depId=? AND name=?",
            "PUBLIC, DEVELOPER", NAME_DEPID_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33)),
            3, "Mozart"
        );
    }



    /** */
    @Test
    public void testComplexIndexCondition3() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'",
            "PUBLIC, DEVELOPER", NAME_DEPID_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition4() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Leipzig'",
            "PUBLIC, DEVELOPER", NAME_DEPID_CITY_IDX, false,
            emptyList()
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition5() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND city='Vienna'",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition6() {
        checkQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition7() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition8() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2 AND age>20",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition9() {
        checkQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId>=2 AND city>='Vienna'",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition10() {
        checkQuery("SELECT * FROM Developer WHERE name>='Mozart' AND city>='Vienna'",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition11() {
        checkQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition12() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'",
            "PUBLIC, DEVELOPER", NAME_DEPID_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition13() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=3 AND city='Vienna'",
            "PUBLIC, DEVELOPER", NAME_CITY_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    /** */
    @Test
    public void testComplexIndexCondition14() {
        checkQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'",
            "PUBLIC, DEVELOPER", DEPID_IDX, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    @Test
    public void testComplexIndexCondition15() {
        checkQuery("SELECT * FROM Developer WHERE age=33 AND city='Vienna'",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33))
        );
    }

    @Test
    public void testEmptyResult() {
        checkQuery("SELECT * FROM Developer WHERE age=33 AND city='Leipzig'",
            "PUBLIC, DEVELOPER", PK, false,
            emptyList()
        );
    }

    @Test
    public void testOrCondition() {
        checkQuery("SELECT * FROM Developer WHERE name='Mozart' OR depId=1",
            "PUBLIC, DEVELOPER", PK, false,
            asList(
                asList(1, "Mozart", 3, "Vienna", 33),
                asList(3, "Bach", 1, "Leipzig", 55))
        );
    }

    /** */
    public void checkQuery(String qry, String tblName, String idxName,boolean ordered, List<List> expRes,
        Object... params) {
        // Check plan.
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> explainCursors =
            engine.query(null, "PUBLIC", "EXPLAIN PLAN FOR " + qry);

        FieldsQueryCursor<List<?>> explainCursor = explainCursors.get(0);
        List<List<?>> explainRes = explainCursor.getAll();
        String plan = (String)explainRes.get(0).get(0);

        String idxScanName = "IgniteTableScan(table=[[" + tblName + "]], index=[" + idxName + ']';

        assertTrue("idxName=" + idxName + ", plan=" + plan, plan.contains(idxScanName));

        // Check result set.
        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", qry, params);

        FieldsQueryCursor<List<?>> cur = cursors.get(0);

        List<List<?>> res = cur.getAll();

        if (!ordered) {
            // Avoid arbitrary order.
            res.sort(new ListComparator());
            explainRes.sort(new ListComparator());
        }

        assertEqualsCollections(expRes, res);
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
