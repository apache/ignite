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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Fail backup node on not entry modified operation.
 * It shold to lead mapping issue on FULL_SYNC cache only, bacause in this case cliwnt try to map operation to nodes locally.
 */
public class FailBackupOnAtomicOperationTest extends GridCommonAbstractTest {
    /** Persistence. */
    private boolean persistence = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(persistence)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(2)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        persistence = false;

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testNodeFailOnCacheReadWithPersistent() throws Exception {
        persistence = true;
        nodeFailOnCacheOp();
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testNodeFailOnCacheReadWithoutPersistent() throws Exception {
        nodeFailOnCacheOp();
    }

    /**
     *
     */
    public void nodeFailOnCacheOp() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        Ignite igniteClient = startClientGrid("client");

        awaitPartitionMapExchange();

        IgniteCache cache = igniteClient.cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = primaryKeys(ignite(1).cache(cache.getName()), 3, 0);

        for (Integer key : keys) {
            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    int i = keys.indexOf(key);

                    switch (keys.indexOf(key)) {
                        case 0:
                            info("Cmd: getAndReplace");

                            cache.getAndReplace(key, key);

                            break;
                        case 1:
                            info("Cmd: invoke");

                            cache.invoke(key, (entry, arguments) -> 0);

                            break;
                        case 2:
                            info("Cmd: remove");

                            cache.remove(key);

                            break;
                        default:
                            fail("The op: " + i + " is not implemented");
                    }
                }
            });

            TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)ignite(1).configuration().getCommunicationSpi();

            communicationSpi.blockMessages(GridNearAtomicUpdateResponse.class, "client");

            communicationSpi.waitForBlocked();

            ignite0.close();

            communicationSpi.stopBlock();

            try {
                fut.get(10_000);
            }
            catch (IgniteCheckedException e) {
                fail("Atomic update hangs. " + e.getMessage());
            }

            startGrid(0);

            awaitPartitionMapExchange();
        }
    }
}
