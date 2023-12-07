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

package org.apache.ignite.internal.cdc;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.cdc.SqlCdcTest.executeSql;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests cache events for CDC.
 */
@RunWith(Parameterized.class)
public class CacheEventsCdcTest extends AbstractCdcTest {
    /** Ignite node. */
    private IgniteEx node;

    /** */
    private IgniteCache<Integer, Integer> dummy;

    /** CDC future. */
    private IgniteInternalFuture<?> cdcFut;

    /** Consumer. */
    private TrackCacheEventsConsumer cnsmr;

    /** */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** */
    @Parameterized.Parameters(name = "persistence={0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setCdcEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        node = startGrid();

        node.cluster().state(ClusterState.ACTIVE);

        cnsmr = new TrackCacheEventsConsumer();

        cdcFut = runAsync(createCdc(cnsmr, node.configuration()));

        dummy = node.getOrCreateCache("dummy");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cdcFut != null) {
            assertFalse(cdcFut.isDone());

            cdcFut.cancel();
        }

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCreateDestroyCache() throws Exception {
        node.createCache(DEFAULT_CACHE_NAME);

        assertTrue(waitForCondition(() -> cnsmr.evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));

        node.destroyCache(DEFAULT_CACHE_NAME);

        assertTrue(waitForCondition(() -> !cnsmr.evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
    }

    /** */
    @Test
    public void testCreateDestroyCachesInGroup() throws Exception {
        String otherCache = "other-cache";

        node.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setGroupName("group"));
        node.createCache(new CacheConfiguration<Integer, Integer>(otherCache).setGroupName("group"));

        dummy.put(1, 1); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(() -> cnsmr.evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
        assertTrue(waitForCondition(() -> cnsmr.evts.containsKey(CU.cacheId(otherCache)), getTestTimeout()));

        node.destroyCache(DEFAULT_CACHE_NAME);

        dummy.put(2, 2); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(() -> !cnsmr.evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
        assertTrue(cnsmr.evts.containsKey(CU.cacheId(otherCache)));

        node.destroyCache(otherCache);

        dummy.put(3, 3); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(() -> !cnsmr.evts.containsKey(CU.cacheId(otherCache)), getTestTimeout()));
        assertFalse(cnsmr.evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)));
    }

    /** */
    @Test
    public void testCreateDropSQLTable() throws Exception {
        AtomicReference<QueryEntity> fromCfg = new AtomicReference<>();

        Function<Integer, GridAbsPredicate> checker = fldCnt -> () -> {
            CdcCacheEvent evt = cnsmr.evts.get(CU.cacheId("T1"));

            if (evt == null)
                return false;

            assertNotNull(evt.configuration().getQueryEntities());
            assertNotNull(evt.queryEntities());

            assertEquals(1, evt.queryEntities().size());

            QueryEntity qryEntity = evt.queryEntities().iterator().next();

            if (qryEntity.getFields().size() != fldCnt)
                return false;

            QueryEntity fromCfg0 = evt.configuration().getQueryEntities().iterator().next();

            fromCfg.set(fromCfg0);

            assertTrue(qryEntity.getFields().containsKey("ID"));
            assertTrue(qryEntity.getFields().containsKey("NAME"));

            assertTrue(fromCfg0.getFields().containsKey("ID"));
            assertTrue(fromCfg0.getFields().containsKey("NAME"));

            assertFalse(
                "Saved config must not change on schema change",
                fromCfg0.getFields().containsKey("CITY_ID")
            );

            if (fldCnt == 3)
                assertTrue(qryEntity.getFields().containsKey("CITY_ID"));

            assertTrue(qryEntity.getIndexes().isEmpty());
            assertTrue(fromCfg0.getIndexes().isEmpty());

            return true;
        };

        executeSql(node, "CREATE TABLE T1(ID INT, NAME VARCHAR, PRIMARY KEY (ID)) WITH \"CACHE_NAME=T1\"");

        dummy.put(1, 1); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(checker.apply(2), getTestTimeout()));

        executeSql(node, "ALTER TABLE T1 ADD COLUMN CITY_ID INT");

        dummy.put(2, 2); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(checker.apply(3), getTestTimeout()));

        executeSql(node, "CREATE INDEX I1 ON T1(CITY_ID)");

        dummy.put(3, 3); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(() -> {
            CdcCacheEvent evt = cnsmr.evts.get(CU.cacheId("T1"));

            QueryEntity qryEntity = evt.queryEntities().iterator().next();

            if (F.isEmpty(qryEntity.getIndexes()))
                return false;

            QueryEntity fromCfg0 = evt.configuration().getQueryEntities().iterator().next();

            assertEquals(fromCfg.get(), fromCfg0);
            assertTrue(fromCfg0.getIndexes().isEmpty());

            QueryIndex idx = qryEntity.getIndexes().iterator().next();

            assertEquals("I1", idx.getName());
            assertEquals(1, idx.getFields().size());
            assertEquals("CITY_ID", idx.getFields().keySet().iterator().next());

            return true;
        }, getTestTimeout()));

        executeSql(node, "DROP TABLE T1");

        dummy.put(4, 4); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(() -> !cnsmr.evts.containsKey(CU.cacheId("T1")), getTestTimeout()));
    }

    /** */
    @Test
    public void testCreateTableForExistingCache() throws Exception {
        Function<Boolean, GridAbsPredicate> checker = chkTblExist -> () -> {
            CdcCacheEvent evt = cnsmr.evts.get(CU.cacheId(DEFAULT_CACHE_NAME));

            if (evt == null)
                return false;

            if (!chkTblExist)
                return true;

            if (F.isEmpty(evt.queryEntities()))
                return false;

            assertEquals(1, evt.queryEntities().size());

            QueryEntity qryEntity = evt.queryEntities().iterator().next();

            if (qryEntity.getFields().size() != 2)
                return false;

            assertTrue(qryEntity.getFields().containsKey("ID"));
            assertTrue(qryEntity.getFields().containsKey("NAME"));

            return true;
        };

        node.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME));

        dummy.put(5, 5); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(checker.apply(false), getTestTimeout()));

        executeSql(
            node,
            "CREATE TABLE T1(ID INT, NAME VARCHAR, PRIMARY KEY (ID)) WITH \"CACHE_NAME=" + DEFAULT_CACHE_NAME + "\""
        );

        dummy.put(6, 6); // Dummy entry to force automatic WAL rollover.

        assertTrue(waitForCondition(checker.apply(true), getTestTimeout()));
    }
}
