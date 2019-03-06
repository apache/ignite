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

package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for igfs with one node in client mode.
 */
public class IgfsOneClientNodeTest extends GridCommonAbstractTest {
    /** Regular cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setForceServerMode(true)
            .setIpFinder(new TcpDiscoveryVmIpFinder(true)));

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName("igfs");
        igfsCfg.setMetaCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));
        igfsCfg.setDataCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration(@NotNull String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        cacheCfg.setCacheMode(PARTITIONED);

        cacheCfg.setBackups(0);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartIgfs() throws Exception {
        final IgfsImpl igfs = (IgfsImpl) grid(0).fileSystem("igfs");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsAbstractSelfTest.create(igfs, new IgfsPath[]{new IgfsPath("/dir")}, null);
                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsPath FILE = new IgfsPath(new IgfsPath("/dir"), "file");

                igfs.delete(FILE, false);

                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsPath FILE = new IgfsPath(new IgfsPath("/dir"), "file");

                igfs.append(FILE, true);

                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");
    }
}
