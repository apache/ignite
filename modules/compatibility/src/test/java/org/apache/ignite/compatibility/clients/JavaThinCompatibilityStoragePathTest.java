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

package org.apache.ignite.compatibility.clients;

import java.io.File;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.junit.Assume;

import static org.apache.ignite.compatibility.clients.JavaThinCompatibilityTest.ADDR;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests java thin client compatibility. This test only checks that thin client can perform basic operations with
 * different client and server versions. Whole API not checked, corner cases not checked.
 */
public class JavaThinCompatibilityStoragePathTest extends AbstractClientCompatibilityTest {
    /** */
    private static final String[] STORAGE_PATH = {"two", "three"};

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(false)
            .setCacheConfiguration(new CacheConfiguration<>("nodeCache").setIndexPath("one").setStoragePaths(STORAGE_PATH))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                .setExtraStoragePaths("one", "two", "three"));
    }

    /** {@inheritDoc} */
    @Override protected void processRemoteConfiguration(IgniteConfiguration cfg) {
        super.processRemoteConfiguration(cfg);

        cfg.setCacheConfiguration(new CacheConfiguration<>("nodeCache"))
            .setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.delete(new File(U.defaultWorkDirectory()));
        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override public void testOldClientToCurrentServer() throws Exception {
        Assume.assumeTrue("Cluster state API exists only from 2.9.0 release", ver.compareTo(VER_2_9_0) >= 0);

        super.testOldClientToCurrentServer();
    }

    /** {@inheritDoc} */
    @Override public void testCurrentClientToOldServer() throws Exception {
        Assume.assumeTrue("Cluster state API exists only from 2.9.0 release", ver.compareTo(VER_2_9_0) >= 0);

        super.testCurrentClientToOldServer();
    }


    /** {@inheritDoc} */
    @Override protected void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer) throws Exception {
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            boolean storagePathSupportedBySrv = serverVer.compareTo(VER_2_18_0) >= 0;
            boolean storagePathSupportedByClient = clientVer.compareTo(VER_2_18_0) >= 0;

            cli.cluster().state(ClusterState.ACTIVE);

            doTestClientCache(cli, storagePathSupportedByClient, storagePathSupportedBySrv);
            doTestNodeCache(cli, storagePathSupportedByClient, storagePathSupportedBySrv);
        }
    }

    /** */
    private void doTestNodeCache(IgniteClient cli, boolean storagePathSupportedByClient, boolean storagePathSupportedBySrv) {
        ClientCache<Object, Object> cache = cli.cache("nodeCache");

        IntStream.range(0, 100).forEach(i -> cache.put(i, i));

        ClientCacheConfiguration ccfg = cache.getConfiguration();

        assertEquals("Must be able to receive config from server", "nodeCache", ccfg.getName());

        checkConfig(storagePathSupportedByClient, storagePathSupportedBySrv, ccfg);
    }

    /** */
    private void doTestClientCache(IgniteClient cli, boolean storagePathSupportedByClient, boolean storagePathSupportedBySrv) {
        ClientCacheConfiguration ccfg = new ClientCacheConfiguration().setName("clientCache");

        if (storagePathSupportedByClient) {
            ccfg.setIndexPath("one")
                .setStoragePaths(STORAGE_PATH);

            if (!storagePathSupportedBySrv) {
                assertThrows(null, () -> cli.createCache(ccfg), RuntimeException.class, "Cache storages are not supported by the server");

                ccfg.setStoragePaths((String[])null);
                ccfg.setIndexPath(null);
            }
        }

        ClientCache<Object, Object> cliCache = cli.createCache(ccfg);

        IntStream.range(0, 100).forEach(i -> cliCache.put(i, i));

        ClientCacheConfiguration cfg = cliCache.getConfiguration();

        checkConfig(storagePathSupportedByClient, storagePathSupportedBySrv, cfg);
    }

    /** */
    private static void checkConfig(
        boolean storagePathSupportedByClient,
        boolean storagePathSupportedBySrv,
        ClientCacheConfiguration ccfg
    ) {
        if (!storagePathSupportedByClient)
            return;

        if (storagePathSupportedBySrv) {
            assertEquals("one", ccfg.getIndexPath());
            assertTrue(Arrays.compare(STORAGE_PATH, ccfg.getStoragePaths()) == 0);
        }
        else {
            assertNull(ccfg.getStoragePaths());
            assertNull(ccfg.getIndexPath());
        }
    }
}
