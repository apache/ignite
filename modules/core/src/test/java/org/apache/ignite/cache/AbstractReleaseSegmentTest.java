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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Base class for testing the release of segments when performing an operation.
 */
public abstract class AbstractReleaseSegmentTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setWalSegmentSize((int)(2 * U.MB))
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 2))
                    .setBackups(1)
            );
    }

    /**
     * Populates the given cache and forces a new checkpoint every 100 updates.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     * @param o Key offset.
     * @throws Exception If failed.
     */
    protected void populate(IgniteCache<Integer, ? super Object> cache, int cnt, int o) throws Exception {
        for (int i = 0; i < cnt; i++) {
            if (i % 100 == 0)
                forceCheckpoint();

            cache.put(i + o, new byte[64 * 1024]);
        }
    }

    /**
     * Releases WAL segment.
     *
     * @param n Node.
     * @param reserved Reserved segment.
     */
    protected void release(IgniteEx n, @Nullable WALPointer reserved) {
        while (reserved != null && walMgr(n).reserved(reserved))
            walMgr(n).release(reserved);
    }

    /**
     * Returns an instance of {@link SegmentAware} for the given ignite node.
     *
     * @return Segment aware.
     */
    protected SegmentAware segmentAware(IgniteEx n) {
        return getFieldValue(walMgr(n), "segmentAware");
    }
}
