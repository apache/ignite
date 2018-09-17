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

package org.apache.ignite.internal.processors.query.h2;

import java.io.File;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Index rebuild after node restart test.
 */
public class GridIndexRebuildWithMvccEnabledSelfTest extends GridIndexRebuildSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration serverConfiguration(int idx, boolean filter) throws Exception {
        return super.serverConfiguration(idx, filter)
            .setMvccVacuumFrequency(Integer.MAX_VALUE);
    }

    /** {@inheritDoc} */
    public void testIndexRebuild() throws Exception {
        IgniteEx srv = startServer();

        execute(srv, "CREATE TABLE T(k int primary key, v int) WITH \"cache_name=T,wrap_value=false," +
            "atomicity=transactional_snapshot\"");

        execute(srv, "CREATE INDEX IDX ON T(v)");

        IgniteInternalCache cc = srv.cachex(CACHE_NAME);

        assertNotNull(cc);

        lockVersion(srv);

        putData(srv, false);

        checkDataState(srv, false);

        File cacheWorkDir = ((FilePageStoreManager)cc.context().shared().pageStore()).cacheWorkDir(cc.configuration());

        File idxPath = cacheWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPath));

        srv = startServer();

        putData(srv, true);

        checkDataState(srv, true);
    }

    /**
     * Lock coordinator version in order to keep MVCC versions in place.
     *
     * @param node Node.
     * @throws IgniteCheckedException if failed.
     */
    private static void lockVersion(IgniteEx node) throws IgniteCheckedException {
        node.context().coordinators().requestSnapshotAsync().get();
    }

    /** {@inheritDoc} */
    protected void checkDataState(IgniteEx srv, boolean afterRebuild) throws IgniteCheckedException {
        IgniteInternalCache icache = srv.cachex(CACHE_NAME);

        assertNotNull(icache);

        CacheObjectContext coCtx = icache.context().cacheObjectContext();

        for (IgniteCacheOffheapManager.CacheDataStore store : icache.context().offheap().cacheDataStores()) {
            GridCursor<? extends CacheDataRow> cur = store.cursor();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                int key = row.key().value(coCtx, false);

                List<IgniteBiTuple<Object, MvccVersion>> vers = store.mvccFindAllVersions(icache.context(), row.key());

                if (!afterRebuild || key <= AMOUNT / 2)
                    assertEquals(key, vers.size());
                else {
                    // For keys affected by concurrent put there are two versions -
                    // -1 (concurrent put mark) and newest restored value as long as put cleans obsolete versions.
                    assertEquals(2, vers.size());

                    Object val0 = ((CacheObject)vers.get(0).getKey()).value(coCtx, false);
                    Object val1 = ((CacheObject)vers.get(1).getKey()).value(coCtx, false);

                    assertEquals(-1, val0);
                    assertEquals(key, val1);
                }

            }
        }
    }
}
