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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.security.SecureRandom;
import java.util.Random;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteFreeListImplTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10 * 1024 * 1024)
                    .setMetricsEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(4 * 1024);

        log.info("***** memCfg[" + gridName + "]: " + memCfg);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception
     */
    public void testAddForRecycle() throws Exception {
        try {
            IgniteEx ignite0 = startGrid(0);

            IgniteCache<Object, Object> cache = ignite0.getOrCreateCache("c1");

            Random rnd = SecureRandom.getInstanceStrong();

            int i = 1;
            try {
                while (i < 198) {
                    cache.put(i, new byte[10000 /* (rnd.nextInt(100) + 1)*/]);
                    i++;
                }

                IgniteEx ignite1 = startGrid(1);
                ignite1.cluster().active(true);

                Thread.sleep(5000);
            }
            catch (Exception e) {
                log.error("Error: ", e);
            }
            finally {
                log.info("Cache put count: " + i);

                /* FreeList fl = ignite0.context().cache().context().database().freeList("default");

                CacheFreeListImpl flImpl = (CacheFreeListImpl)fl;

                PagesList.Stripe[][] buckets = new PagesList.Stripe[256][];

                int stripeCnt = 0;

                log.info("***** shift: " + U.field(flImpl, "shift"));

                for (int j = 0; j < 256; j++) {
                    buckets[j] = U.invoke(AbstractFreeList.class, flImpl, "getBucket", j);

                    if (buckets[j] != null) {
                        stripeCnt += buckets[j].length;

                        log.warning("Non-null bucket: " + j);
                    }
                }

                log.info("Total stripe count: " + stripeCnt);

                DataRegionMetrics drm = ignite0.dataRegionMetrics("default");

                assert drm != null;

                log.info("***** drm: " + drm.getName());
                log.info("  getAllocationRate: " + drm.getAllocationRate());
                log.info("  getDirtyPages: " + drm.getDirtyPages());
                log.info("  getEvictionRate: " + drm.getEvictionRate());
                log.info("  getLargeEntriesPagesPercentage: " + drm.getLargeEntriesPagesPercentage());
                log.info("  getPagesFillFactor: " + drm.getPagesFillFactor());
                log.info("  getPagesReplaceAge: " + drm.getPagesReplaceAge());
                log.info("  getPhysicalMemoryPages: " + drm.getPhysicalMemoryPages());
                log.info("  getTotalAllocatedPages: " + drm.getTotalAllocatedPages()); */
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
