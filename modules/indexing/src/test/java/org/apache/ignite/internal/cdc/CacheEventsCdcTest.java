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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
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

    /** CDC future. */
    private IgniteInternalFuture<?> cdcFut;

    /** Cache events. */
    Map<Integer, CdcCacheEvent> evts = new ConcurrentHashMap<>();

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
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
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

        cdcFut = runAsync(createCdc(new TrackCacheEventsConsumer(), node.configuration()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cdcFut != null) {
            assertFalse(cdcFut.isDone());

            cdcFut.cancel();
        }

        stopAllGrids();

        cleanPersistenceDir();

        evts.clear();
    }

    /** */
    @Test
    public void testCreateDestroyCache() throws Exception {
        node.createCache(DEFAULT_CACHE_NAME);

        assertTrue(waitForCondition(() -> evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));

        node.destroyCache(DEFAULT_CACHE_NAME);

        assertTrue(waitForCondition(() -> !evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
    }

    /** */
    @Test
    public void testCreateDestroyCachesInGroup() throws Exception {
        String otherCache = "other-cache";

        node.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setGroupName("group"));
        node.createCache(new CacheConfiguration<Integer, Integer>(otherCache).setGroupName("group"));

        assertTrue(waitForCondition(() -> evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
        assertTrue(waitForCondition(() -> evts.containsKey(CU.cacheId(otherCache)), getTestTimeout()));

        node.destroyCache(DEFAULT_CACHE_NAME);

        assertTrue(waitForCondition(() -> !evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)), getTestTimeout()));
        assertTrue(evts.containsKey(CU.cacheId(otherCache)));

        node.destroyCache(otherCache);

        assertTrue(waitForCondition(() -> !evts.containsKey(CU.cacheId(otherCache)), getTestTimeout()));
        assertFalse(evts.containsKey(CU.cacheId(DEFAULT_CACHE_NAME)));
    }

    /** */
    @Test
    public void testCreateDropSQLTable() throws Exception {
        executeSql(node, "CREATE TABLE T1(ID INT, NAME VARCHAR, PRIMARY KEY (ID)) WITH \"CACHE_NAME=T1\"");

        Function<Integer, GridAbsPredicate> checker = fldCnt -> () -> {
            CdcCacheEvent evt = evts.get(CU.cacheId("T1"));

            if (evt == null)
                return false;

            assertNotNull(evt.queryEntyties());
            assertEquals(1, evt.queryEntyties().size());

            QueryEntity qryEntity = evt.queryEntyties().iterator().next();

            if (qryEntity.getFields().size() != fldCnt)
                return false;

            assertTrue(qryEntity.getFields().containsKey("ID"));
            assertTrue(qryEntity.getFields().containsKey("NAME"));

            if (fldCnt == 3)
                assertTrue(qryEntity.getFields().containsKey("CITY_ID"));

            assertTrue(qryEntity.getIndexes().isEmpty());

            return true;
        };

        assertTrue(waitForCondition(checker.apply(2), getTestTimeout()));

        executeSql(node, "ALTER TABLE T1 ADD COLUMN CITY_ID INT");

        assertTrue(waitForCondition(checker.apply(3), getTestTimeout()));

        executeSql(node, "CREATE INDEX I1 ON T1(CITY_ID)");

        assertTrue(waitForCondition(() -> {
            CdcCacheEvent evt = evts.get(CU.cacheId("T1"));

            QueryEntity qryEntity = evt.queryEntyties().iterator().next();

            if (qryEntity.getIndexes().isEmpty())
                return false;

            QueryIndex idx = qryEntity.getIndexes().iterator().next();

            assertEquals("I1", idx.getName());
            assertEquals(1, idx.getFields().size());
            assertEquals("CITY_ID", idx.getFields().keySet().iterator().next());

            return true;
        }, getTestTimeout()));

        executeSql(node, "DROP TABLE T1");

        assertTrue(waitForCondition(() -> !evts.containsKey(CU.cacheId("T1")), getTestTimeout()));
    }

    /** */
    @Test
    public void testCreateTableForExistingCache() throws Exception {
        node.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME));

        Function<Boolean, GridAbsPredicate> checker = chkTblExist -> () -> {
            CdcCacheEvent evt = evts.get(CU.cacheId(DEFAULT_CACHE_NAME));

            if (evt == null)
                return false;

            if (!chkTblExist)
                return true;

            assertNotNull(evt.queryEntyties());
            assertEquals(1, evt.queryEntyties().size());

            QueryEntity qryEntity = evt.queryEntyties().iterator().next();

            if (qryEntity.getFields().size() != 2)
                return false;

            assertTrue(qryEntity.getFields().containsKey("ID"));
            assertTrue(qryEntity.getFields().containsKey("NAME"));

            return true;
        };

        executeSql(
            node,
            "CREATE TABLE T1(ID INT, NAME VARCHAR, PRIMARY KEY (ID)) WITH \"CACHE_NAME=" + DEFAULT_CACHE_NAME + "\""
        );
    }

    /** */
    private class TrackCacheEventsConsumer implements CdcConsumer {
        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            cacheEvents.forEachRemaining(e -> evts.put(e.cacheId(), e));
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(evts::remove);
        }

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> evts) {
            evts.forEachRemaining(e -> { /* No-op. */ });

            return false;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(e -> { /* No-op. */ });
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(e -> { /* No-op. */ });
        }


        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op.
        }
    }
}
