/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsNoActualWalHistoryTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setWalSegmentSize(4 * 1024 * 1024)
            .setWalHistorySize(2)
            .setWalSegments(10)
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true));

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testWalBig() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            Random rnd = new Random();

            Map<Integer, IndexedObject> map = new HashMap<>();

            for (int i = 0; i < 40_000; i++) {
                if (i % 1000 == 0)
                    X.println(" >> " + i);

                int k = rnd.nextInt(300_000);
                IndexedObject v = new IndexedObject(rnd.nextInt(10_000));

                cache.put(k, v);
                map.put(k, v);
            }

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context()
                .database();

            // Create many checkpoints to clean up the history.
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();
            dbMgr.wakeupForCheckpoint("test").get();

            dbMgr.enableCheckpoints(false).get();

            for (int i = 0; i < 50; i++) {
                int k = rnd.nextInt(300_000);
                IndexedObject v = new IndexedObject(rnd.nextInt(10_000));

                cache.put(k, v);
                map.put(k, v);
            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.active(true);

            cache = ignite.cache(CACHE_NAME);

            // Check.
            for (Integer k : map.keySet())
                assertEquals(map.get(k), cache.get(k));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        private byte[] payload = new byte[1024];

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }

    /**
     *
     */
    private enum EnumVal {
        /** */
        VAL1,

        /** */
        VAL2,

        /** */
        VAL3
    }
}
