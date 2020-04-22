/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Cluster-wide snapshot test with indexes.
 */
public class IgniteClusterSnapshotWithIndexesTest extends AbstractSnapshotSelfTest {
    /** */
    private CacheConfiguration<Integer, Account> indexedCcfg =
        txCacheConfig(new CacheConfiguration<Integer, Account>("indexed"))
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), Account.class.getName())
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("balance", Integer.class.getName(), null)
                    .setIndexes(F.asList(new QueryIndex("id"),
                        new QueryIndex("balance")))));

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithIndex() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, key -> new Account(key, key), indexedCcfg);

        String tblName =  "Person";

        executeSql(ignite, "CREATE TABLE " + tblName + " (id int, name varchar, age int, city varchar, " +
            "primary key (id, name)) WITH \"affinity_key=name\"");
        executeSql(ignite, "CREATE INDEX ON " + tblName + "(city, age)");

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            executeSql(ignite, "INSERT INTO " + tblName + " (id, name, age, city) VALUES(?, 'name', 3, 'city')", i);

        assertEquals(CACHE_KEYS_RANGE, executeSql(ignite, selectStartSQLStatement(tblName)).size());
        assertEquals(CACHE_KEYS_RANGE, executeSql(ignite.context().cache().jcache(indexedCcfg.getName()),
            selectStartSQLStatement(Account.class.getSimpleName())).size());

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        List<List<?>> results = executeSql(snp, explainSQLStatement(tblName) + "id=0");
        assertUsingPkIndex(results);

        results = executeSql(snp, explainSQLStatement(tblName) + "city='city' and age=2");
        assertUsingSecondaryIndex(results);

        results = executeSql(snp.context().cache().jcache(indexedCcfg.getName()),
            explainSQLStatement(Account.class.getSimpleName()) + "id=0");
        assertUsingSecondaryIndex(results);

        assertEquals(CACHE_KEYS_RANGE, executeSql(snp, selectStartSQLStatement(tblName)).size());
        assertEquals(CACHE_KEYS_RANGE, executeSql(snp.context().cache().jcache(indexedCcfg.getName()),
            selectStartSQLStatement(Account.class.getSimpleName())).size());
    }

    /**
     * @param name Table name;
     * @return Select statement.
     */
    private static String selectStartSQLStatement(String name) {
        return "SELECT * FROM " + name;
    }

    /**
     * @param name Table name.
     * @return Explain statement.
     */
    private static String explainSQLStatement(String name) {
        return "explain SELECT * FROM " + name + " WHERE ";
    }

    /**
     * @param ignite Ignite instance to execute query on.
     * @param stmt Statement to run.
     * @param args Arguments of statement.
     * @return Run result.
     */
    private static List<List<?>> executeSql(IgniteEx ignite, String stmt, Object... args) {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * @param cache Cache to query.
     * @param stmt Statement to run.
     * @return Run result.
     */
    private static List<List<?>> executeSql(IgniteCache<?, ?> cache, String stmt) {
        return cache.query(new SqlFieldsQuery(stmt)).getAll();
    }

    /**
     * @param results Result of execute explain plan query.
     */
    private static void assertUsingSecondaryIndex(List<List<?>> results) {
        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan, explainPlan.toUpperCase().contains("_IDX"));
        assertFalse(explainPlan, explainPlan.toUpperCase().contains("_SCAN_"));
    }

    /**
     * @param results Result of execute explain plan query.
     */
    private static void assertUsingPkIndex(List<List<?>> results) {
        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan.toUpperCase().contains("\"_KEY_PK"));
        assertFalse(explainPlan.toUpperCase().contains("_SCAN_"));
    }

    // todo cache configuration can be changed during SchemaAbstractDiscoveryMessage
}
