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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Date;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsAnyProject;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsAnyScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsUnion;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.generateProxyIdxName;
import static org.hamcrest.CoreMatchers.not;

/**
 * Basic index tests.
 */
public class CalciteBasicSecondaryIndexIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    private static final String PK_IDX_NAME = QueryUtils.PRIMARY_KEY_INDEX;

    /** */
    private static final String AFFINITY_KEY_IDX_NAME = QueryUtils.AFFINITY_KEY_INDEX;

    /** */
    private static final String DEPID_IDX = "DEPID_IDX";

    /** */
    private static final String NAME_CITY_IDX = "NAME_CITY_IDX";

    /** */
    private static final String NAME_DEPID_CITY_IDX = "NAME_DEPID_CITY_IDX";

    /** */
    private static final String DATE_IDX = "DATE_IDX";

    /** */
    private static final String NAME_DATE_IDX = "NAME_DATE_IDX";

    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

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

        IgniteCache<Integer, Developer> devCache = client.createCache(projCfg);

        QueryEntity bdEntity = new QueryEntity();
        bdEntity.setKeyType(Integer.class.getName());
        bdEntity.setKeyFieldName("id");
        bdEntity.setValueType(Birthday.class.getName());
        bdEntity.addQueryField("id", Integer.class.getName(), null);
        bdEntity.addQueryField("name", String.class.getName(), null);
        bdEntity.addQueryField("birthday", Date.class.getName(), null);

        QueryIndex dateIdx = new QueryIndex("birthday", true);
        dateIdx.setName(DATE_IDX);

        LinkedHashMap<String, Boolean> nameDateFields = new LinkedHashMap<>();
        nameDateFields.put("name", false);
        nameDateFields.put("birthday", false);
        QueryIndex nameDateIdx = new QueryIndex(nameDateFields, QueryIndexType.SORTED);
        nameDateIdx.setName(NAME_DATE_IDX);

        bdEntity.setIndexes(asList(dateIdx, nameDateIdx));
        bdEntity.setTableName("Birthday");

        CacheConfiguration<Integer, Birthday> bdCfg = cache(bdEntity);

        IgniteCache<Integer, Birthday> bdCache = client.createCache(bdCfg);

        IgniteCache<CalciteQueryProcessorTest.Key, CalciteQueryProcessorTest.Developer> tblWithAff =
            client.getOrCreateCache(this.<CalciteQueryProcessorTest.Key, CalciteQueryProcessorTest.Developer>cacheConfiguration()
                .setName("TBL_WITH_AFF_KEY")
                .setSqlSchema("PUBLIC")
                .setBackups(1)
                .setQueryEntities(F.asList(new QueryEntity(CalciteQueryProcessorTest.Key.class, CalciteQueryProcessorTest.Developer.class)
                .setTableName("TBL_WITH_AFF_KEY")))
        );

        IgniteCache<Integer, CalciteQueryProcessorTest.Developer> tblConstrPk =
            client.getOrCreateCache(this.<Integer, CalciteQueryProcessorTest.Developer>cacheConfiguration()
                .setName("TBL_CONSTR_PK")
                .setSqlSchema("PUBLIC")
                .setBackups(0)
                .setQueryEntities(F.asList(new QueryEntity(Integer.class, CalciteQueryProcessorTest.Developer.class)
                    .setTableName("TBL_CONSTR_PK")
                    .setKeyFieldName("id")
                    .addQueryField("id", Integer.class.getName(), null)))
            );

        executeSql("CREATE TABLE PUBLIC.UNWRAP_PK" + " (F1 VARCHAR, F2 BIGINT, F3 BIGINT, F4 BIGINT, " +
            "CONSTRAINT PK PRIMARY KEY (F2, F1)) WITH \"backups=0, affinity_key=F1," + atomicity() + "\"");

        put(client, devCache, 1, new Developer("Mozart", 3, "Vienna", 33));
        put(client, devCache, 2, new Developer("Beethoven", 2, "Vienna", 44));
        put(client, devCache, 3, new Developer("Bach", 1, "Leipzig", 55));
        put(client, devCache, 4, new Developer("Strauss", 2, "Munich", 66));

        put(client, devCache, 5, new Developer("Vagner", 4, "Leipzig", 70));
        put(client, devCache, 6, new Developer("Chaikovsky", 5, "Votkinsk", 53));
        put(client, devCache, 7, new Developer("Verdy", 6, "Rankola", 88));
        put(client, devCache, 8, new Developer("Stravinsky", 7, "Spt", 89));
        put(client, devCache, 9, new Developer("Rahmaninov", 8, "Starorussky ud", 70));
        put(client, devCache, 10, new Developer("Shubert", 9, "Vienna", 31));
        put(client, devCache, 11, new Developer("Glinka", 10, "Smolenskaya gb", 53));

        put(client, devCache, 12, new Developer("Einaudi", 11, "", -1));
        put(client, devCache, 13, new Developer("Glass", 12, "", -1));
        put(client, devCache, 14, new Developer("Rihter", 13, "", -1));

        put(client, devCache, 15, new Developer("Marradi", 14, "", -1));
        put(client, devCache, 16, new Developer("Zimmer", 15, "", -1));
        put(client, devCache, 17, new Developer("Hasaishi", 16, "", -1));

        put(client, devCache, 18, new Developer("Arnalds", 17, "", -1));
        put(client, devCache, 19, new Developer("Yiruma", 18, "", -1));
        put(client, devCache, 20, new Developer("O'Halloran", 19, "", -1));

        put(client, devCache, 21, new Developer("Cacciapaglia", 20, "", -1));
        put(client, devCache, 22, new Developer("Prokofiev", 21, "", -1));
        put(client, devCache, 23, new Developer("Musorgskii", 22, "", -1));

        put(client, bdCache, 1, new Birthday("Mozart", Date.valueOf("1756-01-27")));
        put(client, bdCache, 2, new Birthday("Beethoven", null));
        put(client, bdCache, 3, new Birthday("Bach", Date.valueOf("1685-03-31")));
        put(client, bdCache, 4, new Birthday("Strauss", Date.valueOf("1864-06-11")));
        put(client, bdCache, 5, new Birthday("Vagner", Date.valueOf("1813-05-22")));
        put(client, bdCache, 6, new Birthday("Chaikovsky", Date.valueOf("1840-05-07")));
        put(client, bdCache, 7, new Birthday("Verdy", Date.valueOf("1813-10-10")));

        put(client, tblWithAff, new CalciteQueryProcessorTest.Key(1, 2), new CalciteQueryProcessorTest.Developer("Petr", 10));
        put(client, tblWithAff, new CalciteQueryProcessorTest.Key(2, 3), new CalciteQueryProcessorTest.Developer("Ivan", 11));

        put(client, tblConstrPk, 1, new CalciteQueryProcessorTest.Developer("Petr", 10));
        put(client, tblConstrPk, 2, new CalciteQueryProcessorTest.Developer("Ivan", 11));

        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Petr', 1, 2, 3)");
        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan', 2, 2, 4)");

        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan1', 21, 2, 4)");
        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan2', 22, 2, 4)");
        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan3', 23, 2, 4)");
        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan4', 24, 2, 4)");
        executeSql("INSERT INTO PUBLIC.UNWRAP_PK(F1, F2, F3, F4) values ('Ivan5', 25, 2, 4)");

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // Skip super method to keep caches after each test.
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** */
    private <K, V> CacheConfiguration<K, V> cache(QueryEntity ent) {
        return this.<K, V>cacheConfiguration().setName(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    /** */
    @Test
    public void testEqualsFilterWithUnwrpKey() {
        assertQuery("SELECT F1 FROM UNWRAP_PK WHERE F2=2")
            .matches(containsIndexScan("PUBLIC", "UNWRAP_PK", QueryUtils.PRIMARY_KEY_INDEX))
            .returns("Ivan")
            .check();
    }

    /** */
    @Test
    public void testEqualsFilterWithUnwrpKeyAndAff() {
        assertQuery("SELECT F2 FROM UNWRAP_PK WHERE F1='Ivan'")
            .matches(containsIndexScan("PUBLIC", "UNWRAP_PK", QueryUtils.AFFINITY_KEY_INDEX))
            .check();
    }

    /** */
    @Test
    public void testIndexLoopJoin() {
        assertQuery("" +
            "SELECT /*+ " + HintDefinition.CNL_JOIN + " */ d1.name, d2.name " +
            "FROM Developer d1, Developer d2 WHERE d1.id = d2.id")
            .matches(containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .returns("Bach", "Bach")
            .returns("Beethoven", "Beethoven")
            .returns("Mozart", "Mozart")
            .returns("Strauss", "Strauss")
            .returns("Vagner", "Vagner")
            .returns("Chaikovsky", "Chaikovsky")
            .returns("Verdy", "Verdy")
            .returns("Stravinsky", "Stravinsky")
            .returns("Rahmaninov", "Rahmaninov")
            .returns("Shubert", "Shubert")
            .returns("Glinka", "Glinka")
            .returns("Arnalds", "Arnalds")
            .returns("Glass", "Glass")
            .returns("O'Halloran", "O'Halloran")
            .returns("Prokofiev", "Prokofiev")
            .returns("Yiruma", "Yiruma")
            .returns("Cacciapaglia", "Cacciapaglia")
            .returns("Einaudi", "Einaudi")
            .returns("Hasaishi", "Hasaishi")
            .returns("Marradi", "Marradi")
            .returns("Musorgskii", "Musorgskii")
            .returns("Rihter", "Rihter")
            .returns("Zimmer", "Zimmer")
            .check();
    }

    /** */
    @Test
    public void testMergeAndHashJoin() {
        for (List<String> params : F.asList(F.asList(HintDefinition.MERGE_JOIN.name(), "IgniteMergeJoin"),
            F.asList(HintDefinition.HASH_JOIN.name(), "IgniteHashJoin"))) {
            assertQuery("" +
                "SELECT /*+ " + params.get(0) + " */ d1.name, d2.name FROM Developer d1, Developer d2 " +
                "WHERE d1.depId = d2.depId")
                .matches(containsSubPlan(params.get(1)))
                .returns("Bach", "Bach")
                .returns("Beethoven", "Beethoven")
                .returns("Beethoven", "Strauss")
                .returns("Mozart", "Mozart")
                .returns("Strauss", "Strauss")
                .returns("Strauss", "Beethoven")
                .returns("Vagner", "Vagner")
                .returns("Chaikovsky", "Chaikovsky")
                .returns("Verdy", "Verdy")
                .returns("Stravinsky", "Stravinsky")
                .returns("Rahmaninov", "Rahmaninov")
                .returns("Shubert", "Shubert")
                .returns("Glinka", "Glinka")
                .returns("Arnalds", "Arnalds")
                .returns("Glass", "Glass")
                .returns("O'Halloran", "O'Halloran")
                .returns("Prokofiev", "Prokofiev")
                .returns("Yiruma", "Yiruma")
                .returns("Cacciapaglia", "Cacciapaglia")
                .returns("Einaudi", "Einaudi")
                .returns("Hasaishi", "Hasaishi")
                .returns("Marradi", "Marradi")
                .returns("Musorgskii", "Musorgskii")
                .returns("Rihter", "Rihter")
                .returns("Zimmer", "Zimmer")
                .check();
        }
    }

    // ===== No filter =====

    /** */
    @Test
    public void testNoFilter() {
        assertQuery("SELECT * FROM Developer")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    // ===== _key filter =====

    /** */
    @Test
    public void testKeyColumnEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key=1")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX_NAME))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();

        assertQuery("SELECT * FROM Developer WHERE id=1")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testEqualsFilterWithAffIdx() {
        assertQuery("SELECT * FROM TBL_WITH_AFF_KEY WHERE affinityKey=3")
            .matches(containsIndexScan("PUBLIC", "TBL_WITH_AFF_KEY", AFFINITY_KEY_IDX_NAME))
            .returns(2, 3, "Ivan", 11)
            .check();
    }

    /** */
    @Test
    public void testEqualsFilterWithPkIdx1() {
        assertQuery("SELECT * FROM TBL_CONSTR_PK WHERE id=2")
            .matches(containsIndexScan("PUBLIC", "TBL_CONSTR_PK", generateProxyIdxName(PK_IDX_NAME)))
            .returns("Ivan", 11, 2)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key>3 and _key<12")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX_NAME))
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key>=? and _key<=?")
            .withParams(3, 11)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX_NAME))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key<?")
            .withParams(3)
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyColumnLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE _key<=2")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    // ===== alias filter =====

    /** */
    @Test
    public void testKeyAliasEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>? and id<?")
            .withParams(3, 12)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>=3 and id<12")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyAliasLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", generateProxyIdxName(PK_IDX_NAME)))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    // ===== indexed field filter =====

    /** */
    @Test
    public void testIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedDateFieldEqualsFilter() {
        assertQuery("SELECT * FROM Birthday WHERE birthday = DATE '1813-05-22'")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", DATE_IDX))
            .returns(5, "Vagner", Date.valueOf("1813-05-22"))
            .check();
    }

    /** */
    @Test
    public void testIndexedDateFieldEqualsParameterFilter() {
        assertQuery("SELECT * FROM Birthday WHERE birthday = ?")
            .withParams(Date.valueOf("1813-05-22"))
            .returns(5, "Vagner", Date.valueOf("1813-05-22"))
            .check();
    }

    /** */
    @Test
    public void testIndexedDateFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Birthday WHERE birthday > DATE '1813-05-22'")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", DATE_IDX))
            .returns(4, "Strauss", Date.valueOf("1864-06-11"))
            .returns(6, "Chaikovsky", Date.valueOf("1840-05-07"))
            .returns(7, "Verdy", Date.valueOf("1813-10-10"))
            .check();
    }

    /** */
    @Test
    public void testIndexedDateFieldLessThanOrEqualFilter() {
        assertQuery("SELECT * FROM Birthday WHERE birthday <= DATE '1756-01-27'")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", DATE_IDX))
            .returns(1, "Mozart", Date.valueOf("1756-01-27"))
            .returns(3, "Bach", Date.valueOf("1685-03-31"))
            .check();
    }

    /** */
    @Test
    public void testIndexedDateFieldBetweenFilter() {
        assertQuery("SELECT * FROM Birthday WHERE birthday BETWEEN DATE '1756-01-27' AND DATE '1813-10-10'")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", DATE_IDX))
            .returns(1, "Mozart", Date.valueOf("1756-01-27"))
            .returns(5, "Vagner", Date.valueOf("1813-05-22"))
            .returns(7, "Verdy", Date.valueOf("1813-10-10"))
            .check();
    }

    /** */
    @Test
    public void testIndexedNameDateFieldEqualsFilter() {
        assertQuery("SELECT * FROM Birthday WHERE name = 'Vagner' AND birthday = DATE '1813-05-22'")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", NAME_DATE_IDX))
            .returns(5, "Vagner", Date.valueOf("1813-05-22"))
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>21")
            .withParams(3)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>=21")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<?")
            .withParams(3)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
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
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
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
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>?")
            .withParams(50)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>=?")
            .withParams(34)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<?")
            .withParams(56)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<=?")
            .withParams(55)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Test
    public void testComplexIndexCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition2() {
        assertQuery("SELECT * FROM Developer WHERE depId=? AND name=?")
            .withParams(3, "Mozart")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition4() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Leipzig'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition5() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition6() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition7() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition8() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2 AND age>20")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition9() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId>=2 AND city>='Vienna'")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX, DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition10() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND city>='Vienna'")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition11() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition12() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition13() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition14() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition15() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Vienna'")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition16() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND (city='Vienna' AND depId=3)")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testEmptyResult() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Leipzig'")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .check();
    }

    /** */
    @Test
    public void testOrCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' OR age=55")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    public void testOrCondition2() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (depId=1 OR depId=3)")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testOrCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (age > 22 AND (depId=1 OR depId=3))")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testOrCondition4() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR (name='Mozart' AND depId=3)")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    public void testOrCondition5() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR name='Mozart'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Ignore("TODO")
    @Test
    public void testOrderByKey() {
        assertQuery("SELECT id, name, depId, age FROM Developer ORDER BY _key")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .matches(not(containsSubPlan("IgniteSort")))
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
        assertQuery("SELECT * FROM Developer WHERE id<=4 ORDER BY id nulls first")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER"))
            .matches(not(containsSubPlan("IgniteSort")))
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
        assertQuery("SELECT * FROM Developer ORDER BY depId nulls first")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .matches(not(containsSubPlan("IgniteSort")))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)

            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)

            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityAsc() {
        assertQuery("SELECT * FROM Developer ORDER BY name, city")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .matches(containsSubPlan("IgniteSort"))
            .returns(18, "Arnalds", 17, "", -1)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(23, "Musorgskii", 22, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(14, "Rihter", 13, "", -1)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityDesc() {
        assertQuery("SELECT * FROM Developer ORDER BY name DESC, city DESC")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .matches(not(containsSubPlan("IgniteSort")))
            .returns(16, "Zimmer", 15, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(14, "Rihter", 13, "", -1)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(15, "Marradi", 14, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(13, "Glass", 12, "", -1)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(18, "Arnalds", 17, "", -1)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNoIndexedColumn() {
        assertQuery("SELECT * FROM Developer ORDER BY age DESC, ID")
            .matches(containsAnyProject("PUBLIC", "DEVELOPER"))
            .matches(containsSubPlan("IgniteSort"))
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .ordered()
            .check();
    }

    /**
     * Test verifies that ranges would be serialized and desirialized without any errors.
     */
    @Test
    public void testSelectWithRanges() {
        String sql = "select depId from Developer " +
            "where depId in (1,2,3,5,6,7,9,10,13,14,15,18,19,20,21,22,23,24,25,26,27,28,30,31,32,33) " +
            "   or depId between 7 and 8 order by depId limit 5";

        assertQuery(sql)
            .returns(1)
            .returns(2)
            .returns(2)
            .returns(3)
            .returns(5)
            .check();
    }

    /**
     * A test to verify that the planner is able to optimize such a query in
     * a reasonable amount of time.
     *
     * A "reasonable" here is the time less than test's timeout. Despite
     * timeout is too big, it's less than INF though.
     */
    @Test
    public void testToPlanQueryWithAllOperator() {
        assertQuery("SELECT name FROM Developer WHERE age > ALL ( SELECT 88 )")
            .returns("Stravinsky")
            .check();
    }

    /**
     * Test index search bounds merge.
     */
    @Test
    public void testIndexBoundsMerge() {
        assertQuery("SELECT id FROM Developer WHERE depId > 19 AND depId > ?")
            .withParams(20)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(22)
            .returns(23)
            .check();

        assertQuery("SELECT id FROM Developer WHERE depId > 20 AND depId > ?")
            .withParams(19)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(22)
            .returns(23)
            .check();

        assertQuery("SELECT id FROM Developer WHERE depId >= 20 AND depId > ?")
            .withParams(19)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(21)
            .returns(22)
            .returns(23)
            .check();

        assertQuery("SELECT id FROM Developer WHERE depId BETWEEN ? AND ? AND depId > 19")
            .withParams(19, 21)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(21)
            .returns(22)
            .check();

        // Index with DESC ordering.
        assertQuery("SELECT id FROM Birthday WHERE name BETWEEN 'B' AND 'D' AND name > ?")
            .withParams("Bach")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", NAME_DATE_IDX))
            .returns(2)
            .returns(6)
            .check();
    }

    /**
     * Test index search bounds on complex index expression.
     */
    @Test
    public void testComplexIndexExpression() {
        assertQuery("SELECT id FROM Developer WHERE depId BETWEEN ? - 1 AND ? + 1")
            .withParams(20, 20)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(20)
            .returns(21)
            .returns(22)
            .check();

        assertQuery("SELECT id FROM Birthday WHERE name = SUBSTRING(?::VARCHAR, 1, 4)")
            .withParams("BachBach")
            .matches(containsIndexScan("PUBLIC", "BIRTHDAY", NAME_DATE_IDX))
            .returns(3)
            .check();

        assertQuery("SELECT id FROM Birthday WHERE name = SUBSTRING(name, 1, 4)")
            .matches(containsTableScan("PUBLIC", "BIRTHDAY"))
            .returns(3)
            .check();
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

    /** */
    private static class Birthday {
        /** */
        String name;

        /** */
        Date birthday;

        /** */
        public Birthday(String name, Date birthday) {
            this.name = name;
            this.birthday = birthday;
        }
    }

    /** */
    public static class Key {
        /** */
        @QuerySqlField
        public int id;

        /** */
        @QuerySqlField
        public int id2;

        /** */
        public Key(int id, int id2) {
            this.id = id;
            this.id2 = id2;
        }
    }
}
