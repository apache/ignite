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

package org.apache.ignite.internal.processors.cache.index;

import java.io.File;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCounter;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * T.
 */
public class GridIndexRebuildSelfTest extends DynamicIndexAbstractSelfTest {
    /** Data size. */
    private final static int AMOUNT = 10;

    /** Next key to put. */
    private int nextKey = 1;

    /** Next value to put. */
    private int nextVal = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg =  super.commonConfiguration(idx);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        nextKey = 1;

        nextVal = 1;
    }

    /**
     *
     */
    public void testMvccEnabled() throws Exception {
        doTest(true);
    }

    /**
     * Do test.
     * @param mvccEnabled MVCC flag.
     * @throws Exception if failed.
     */
    private void doTest(boolean mvccEnabled) throws Exception {
        IgniteEx srv = startServer(mvccEnabled);

        execute(srv, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=T,wrap_value=false," +
            "atomicity=transactional\"");

        execute(srv, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = srv.cachex("T");

        assertNotNull(cc);

        lockVersion(srv);

        putData(srv);

        checkTreeState(srv, mvccEnabled);

        File cacheWorkDir = ((FilePageStoreManager)cc.context().shared().pageStore()).cacheWorkDir(cc.configuration());

        File idxPath = cacheWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPath));

        srv = startServer(mvccEnabled);

        checkTreeState(srv, mvccEnabled);
    }

    /**
     * Check versions presence in index tree.
     * @param srv Node.
     * @param mvccEnabled MVCC flag.
     * @throws IgniteCheckedException if failed.
     */
    private void checkTreeState(IgniteEx srv, boolean mvccEnabled) throws IgniteCheckedException {
        IgniteInternalCache cc = srv.cachex("T");

        assertNotNull(cc);

        for (IgniteCacheOffheapManager.CacheDataStore store : cc.context().offheap().cacheDataStores()) {
            GridCursor<? extends CacheDataRow> cur = store.cursor();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                List<T2<Object, MvccCounter>> vers = store.mvccFindAllVersions(cc.context(), row.key());

                X.println(row.key().toString(), vers.size());
            }
        }
    }

    /**
     * Lock coordinator version in order to keep MVCC versions in place.
     * @param node Node.
     * @throws IgniteCheckedException if failed.
     */
    private static void lockVersion(IgniteEx node) throws IgniteCheckedException {
        node.context().coordinators().requestQueryCounter(node.context().coordinators().currentCoordinator()).get();
    }

    /**
     * Put data to cache.
     * @param node Node.
     * @throws Exception if failed.
     */
    private void putData(Ignite node) throws Exception {
        final IgniteCache<Integer, Integer> cache = node.cache("T");

        assertNotNull(cache);

        // Data streamer is not used intentionally in order to preserve all versions.
        multithreadedAsync(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                T2<Integer, Integer> t;

                while ((t = nextKeyAndValue()) != null)
                    cache.put(t.getKey(), t.getValue());

                return null;
            }
        }, 4).get();
    }

    /**
     * @return Next key-value pair to put to cache.
     */
    private synchronized T2<Integer, Integer> nextKeyAndValue () {
        if (nextKey <= AMOUNT && nextVal <= AMOUNT) {
            T2<Integer, Integer> res = new T2<>(nextKey, nextVal);

            X.println(nextKey + " as " + nextVal);

            if (nextVal == nextKey) {
                nextVal = 1;

                nextKey++;
            }
            else
                nextVal++;

            return res;
        }
        else
            X.println(nextKey + " ZZZ " + nextVal);

        return null;
    }

    /**
     * Start server node.
     * @param mvccEnabled MVCC flag.
     * @return Started node.
     * @throws Exception if failed.
     */
    private IgniteEx startServer(boolean mvccEnabled) throws Exception {
        IgniteConfiguration cfg = serverConfiguration(0).setMvccEnabled(mvccEnabled);

        IgniteEx res = startGrid(cfg);

        res.active(true);

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
