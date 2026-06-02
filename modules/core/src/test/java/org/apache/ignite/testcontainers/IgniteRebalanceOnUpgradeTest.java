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

package org.apache.ignite.testcontainers;

import java.io.File;
import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.testcontainers.IgniteContainer.LOCAL_WORK_DIR_PATH;
import static org.apache.ignite.testcontainers.IgniteContainer.RollingUpgradeStatus.DISABLED;
import static org.apache.ignite.testcontainers.IgniteContainer.RollingUpgradeStatus.ENABLED;
import static org.junit.Assert.assertEquals;

/** Smoke test for rolling upgrade with persistence. */
public class IgniteRebalanceOnUpgradeTest {
    /** Node IDs. */
    private static final List<String> NODE_IDS = List.of(
        "ad26bff6-5ff5-49f1-9a61-425a827953ed",
        "c1099d16-e7d7-49f4-925c-53329286c444",
        "7b880b69-8a9e-4b84-b555-250d365e2e67"
    );

    /** Source version. */
    private static final String SOURCE_VER = "2.18.0";

    /** Target version for RU. */
    private static final String TARGET_VER = "2.18.1";

    /** Cache name. */
    private static final String CACHE_NAME = "ru-test-cache";

    /** Local work directory. */
    private static final File LOCAL_WORK_DIR = new File(LOCAL_WORK_DIR_PATH);

    /** Thin client. */
    private IgniteClient client;

    /** */
    @BeforeClass
    public static void beforeClass() {
        U.delete(LOCAL_WORK_DIR);
    }

    /** */
    @AfterClass
    public static void afterClass() {
        U.delete(LOCAL_WORK_DIR);
    }

    /** Basic RU test. */
    @Test
    public void testRollingUpgrade() {
        try (IgniteClusterContainer cluster = new IgniteClusterContainer(SOURCE_VER, NODE_IDS)) {
            cluster.start();

            IgniteContainer node = cluster.firstNode();

            node.activateCluster();

            ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ClientCache<Integer, Integer> cache = client(node.clientAddress()).createCache(cfg);

            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            assertEquals(DISABLED, node.rollingUpgradeStatus());

            node.rollingUpgradeEnable(TARGET_VER);

            assertEquals(ENABLED, node.rollingUpgradeStatus());

            closeClient();

            cluster.upgrade(TARGET_VER);

            node = cluster.firstNode();

            assertEquals(NODE_IDS.size(), node.nodesCountForVersion(TARGET_VER));

            node.rollingUpgradeDisable();

            assertEquals(DISABLED, node.rollingUpgradeStatus());

            ClientCache<Integer, Integer> targetCache = client(node.clientAddress()).getOrCreateCache(CACHE_NAME);

            for (int i = 0; i < 1000; i++)
                assertEquals("Data mismatch after upgrade at key: " + i, i, (int)targetCache.get(i));

            targetCache.put(1001, 1001);

            assertEquals(1001, (int)targetCache.get(1001));
        }
        finally {
            closeClient();
        }
    }

    /** */
    private IgniteClient client(String addr) {
        if (client == null)
            client = Ignition.startClient(new ClientConfiguration().setAddresses(addr));

        return client;
    }

    /** */
    private void closeClient() {
        if (client != null) {
            client.close();

            client = null;
        }
    }
}
