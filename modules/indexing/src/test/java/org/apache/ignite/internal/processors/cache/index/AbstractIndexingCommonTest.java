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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.util.CloseWatcher;

import static org.apache.ignite.internal.util.IgniteUtils.awaitQuiet;

/**
 * Base class for all indexing tests to check H2 connection management.
 */
public class AbstractIndexingCommonTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        checkAllConnectionAreClosed();

        super.afterTestsStopped();
    }

    /**
     * Checks all H2 connection are closed.
     */
    void checkAllConnectionAreClosed() {
        Set<Object> refs = GridTestUtils.getFieldValue(CloseWatcher.class, "refs");

        if (!refs.isEmpty()) {
            for (Object o : refs) {
                if (o instanceof CloseWatcher
                    && ((CloseWatcher)o).getCloseable() instanceof Session) {
                    log.error("Session: " + ((CloseWatcher)o).getCloseable()
                        + ", open=" + !((Session)((CloseWatcher)o).getCloseable()).isClosed());
                }
            }

            // Uncomment and use heap dump to investigate the problem if the test failed.
            // GridDebug.dumpHeap("h2_conn_heap_dmp.hprof", true);

            fail("There are not closed connections. See the log above.");
        }
    }

    /**
     * Collects index file paths from all grids for given cache. Must be called when the grid is up.
     *
     * @param cacheName Cache name.
     */
    protected List<Path> getIndexBinPaths(String cacheName) {
        return G.allGrids().stream()
            .map(grid -> (IgniteEx) grid)
            .map(grid -> {
                IgniteInternalCache<Object, Object> cachex = grid.cachex(cacheName);

                assertNotNull(cachex);

                FilePageStoreManager pageStoreMgr = (FilePageStoreManager) cachex.context().shared().pageStore();

                assertNotNull(pageStoreMgr);

                File cacheWorkDir = pageStoreMgr.cacheWorkDir(cachex.configuration());

                return cacheWorkDir.toPath().resolve("index.bin");
            })
            .collect(Collectors.toList());
    }

    /**
     * Blocking indexing processor.
     * <p>
     * Blocks the indexes rebuilding until unblocked via {@link #stopBlock(String)}.
     */
    public static class BlockingIndexing extends IgniteH2Indexing {
        /** */
        private final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override protected void rebuildIndexesFromHash0(
            GridCacheContext cctx,
            SchemaIndexCacheVisitorClosure clo,
            GridFutureAdapter<Void> rebuildIdxFut
        ) {
            CountDownLatch startThread = new CountDownLatch(1);

            new Thread(() -> {
                startThread.countDown();

                new SchemaIndexCacheVisitorImpl(cctx, null, null, rebuildIdxFut) {
                    /** {@inheritDoc} */
                    @Override protected void beforeExecute() {
                        String cacheName = cctx.name();

                        if (log.isInfoEnabled())
                            log.info("Before execute build idx for cache=" + cacheName);

                        awaitQuiet(latches.computeIfAbsent(cacheName, l -> new CountDownLatch(1)));
                    }
                }.visit(clo);
            }).start();

            awaitQuiet(startThread);
        }

        /**
         * Returns whether creating/rebuilding an index for cache is blocked.
         *
         * @return {@code True} if creating/rebuilding an index for cache is
         *      blocked.
         */
        public boolean isBlock(String cacheName) {
            return latches.containsKey(cacheName) && latches.get(cacheName).getCount() != 0;
        }

        /**
         * Stops the indexes rebuilding block for given cache.
         *
         * @param cacheName Cache name.
         */
        public void stopBlock(String cacheName) {
            latches.computeIfAbsent(cacheName, l -> new CountDownLatch(1)).countDown();
        }
    }
}
