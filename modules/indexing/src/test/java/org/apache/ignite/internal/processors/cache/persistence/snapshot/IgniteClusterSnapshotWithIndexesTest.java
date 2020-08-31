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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;

/**
 * Cluster-wide snapshot test with indexes.
 */
public class IgniteClusterSnapshotWithIndexesTest extends AbstractSnapshotSelfTest {
    /** Configuration with statically configured indexes. */
    private final CacheConfiguration<Integer, Account> indexedCcfg =
        txCacheConfig(new CacheConfiguration<Integer, Account>("indexed"))
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), Account.class.getName())
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("balance", Integer.class.getName(), null)
                    .setIndexes(F.asList(new QueryIndex("id"),
                        new QueryIndex("balance")))));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setCheckpointFrequency(DFLT_CHECKPOINT_FREQ);

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithIndexes() throws Exception {
        String tblName = "Person";
        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, key -> new Account(key, key), indexedCcfg);

        executeSql(ignite, "CREATE TABLE " + tblName + " (id int, name varchar, age int, city varchar, " +
            "primary key (id, name)) WITH \"cache_name=" + tblName + "\"");
        executeSql(ignite, "CREATE INDEX ON " + tblName + "(city, age)");

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            executeSql(ignite, "INSERT INTO " + tblName + " (id, name, age, city) VALUES(?, 'name', 3, 'city')", i);

        assertEquals(CACHE_KEYS_RANGE, rowsCount(executeSql(ignite, selectStartSQLStatement(tblName))));
        assertEquals(CACHE_KEYS_RANGE, rowsCount(executeSql(ignite.context().cache().jcache(indexedCcfg.getName()),
            selectStartSQLStatement(Account.class.getSimpleName()))));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        assertTrue(snp.cache(indexedCcfg.getName()).indexReadyFuture().isDone());
        assertTrue(snp.cache(tblName).indexReadyFuture().isDone());

        List<List<?>> results = executeSql(snp, explainSQLStatement(tblName) + "id > 10");

        // Primary key exists.
        String explainPlan = (String)results.get(0).get(0);
        assertTrue(explainPlan.toUpperCase().contains("\"_KEY_PK"));
        assertFalse(explainPlan.toUpperCase().contains("_SCAN_"));

        results = executeSql(snp, explainSQLStatement(tblName) + "city='city' and age=2");
        assertUsingSecondaryIndex(results);

        results = executeSql(snp.context().cache().jcache(indexedCcfg.getName()),
            explainSQLStatement(Account.class.getSimpleName()) + "id=0");
        assertUsingSecondaryIndex(results);

        assertEquals(CACHE_KEYS_RANGE, rowsCount(executeSql(snp, selectStartSQLStatement(tblName))));
        assertEquals(CACHE_KEYS_RANGE, rowsCount(executeSql(snp.context().cache().jcache(indexedCcfg.getName()),
            selectStartSQLStatement(Account.class.getSimpleName()))));

        forceCheckpoint();

        // Validate indexes on start.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(new HashSet<>(Arrays.asList(indexedCcfg.getName(), tblName)),
            0, 0, false, true);

        for (Ignite node : G.allGrids()) {
            ((IgniteEx)node).context().resource().injectGeneric(clo);

            assertFalse(clo.call().hasIssues());
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotConsistentConfig() throws Exception {
        String tblName = "PersonCache";
        int grids = 3;

        IgniteEx ignite = startGridsWithoutCache(grids);

        executeSql(ignite, "CREATE TABLE " + tblName + " (id int, name varchar, age int, city varchar, " +
            "primary key (id, name)) WITH \"cache_name=" + tblName + "\"");
        executeSql(ignite, "CREATE INDEX SNP_IDX_0 ON " + tblName + "(age)");

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            executeSql(ignite, "INSERT INTO " + tblName + " (id, name, age, city) VALUES(?, 'name', 3, 'city')", i);

        // Blocking configuration local snapshot sender.
        List<BlockingExecutor> execs = setBlockingSnapshotExecutor(G.allGrids());

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        List<String> idxNames = Arrays.asList("SNP_IDX_1", "SNP_IDX_2");

        executeSql(ignite, "CREATE INDEX " + idxNames.get(0) + " ON " + tblName + "(city)");
        executeSql(ignite, "CREATE INDEX " + idxNames.get(1) + " ON " + tblName + "(age, city)");

        for (BlockingExecutor exec : execs)
            exec.unblock();

        fut.get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(grids, SNAPSHOT_NAME);

        List<String> currIdxNames = executeSql(snp, "SELECT * FROM SYS.INDEXES").stream().
            map(l -> (String)l.get(0))
            .collect(Collectors.toList());

        assertTrue("Concurrently created indexes must not exist in the snapshot: " + currIdxNames,
            Collections.disjoint(idxNames, currIdxNames));

        List<List<?>> results = executeSql(snp, explainSQLStatement(tblName) + "age=2");
        assertUsingSecondaryIndex(results);
    }

    /**
     * @param name Table name;
     * @return Select statement.
     */
    private static String selectStartSQLStatement(String name) {
        return "SELECT count(*) FROM " + name;
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
     * @param res Statement results.
     * @return Number of rows.
     */
    private static long rowsCount(List<List<?>> res) {
        return (Long)res.get(0).get(0);
    }

    /**
     * @param results Result of execute explain plan query.
     */
    private static void assertUsingSecondaryIndex(List<List<?>> results) {
        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan, explainPlan.toUpperCase().contains("_IDX"));
        assertFalse(explainPlan, explainPlan.toUpperCase().contains("_SCAN_"));
    }
}
