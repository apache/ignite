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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgfsStartCacheTest extends IgfsCommonAbstractTest {
    /**
     * @param igfs If {@code true} created IGFS configuration.
     * @param idx Node index.
     * @return Configuration
     */
    private IgniteConfiguration config(boolean igfs, int idx) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        if (igfs) {
            FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

            igfsCfg.setName("igfs");
            igfsCfg.setDefaultMode(PRIMARY);
            igfsCfg.setFragmentizerEnabled(false);

            CacheConfiguration dataCacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            dataCacheCfg.setCacheMode(PARTITIONED);
            dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
            dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(1));

            CacheConfiguration metaCacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            metaCacheCfg.setCacheMode(REPLICATED);
            metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
            metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            igfsCfg.setMetaCacheConfiguration(metaCacheCfg);
            igfsCfg.setDataCacheConfiguration(dataCacheCfg);

            cfg.setFileSystemConfiguration(igfsCfg);
        }

        cfg.setIgniteInstanceName("node-" + idx);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStart() throws Exception {
        Ignite g0 = G.start(config(true, 0));

        String dataCacheName = ((IgniteEx)g0).igfsx("igfs").configuration().getDataCacheConfiguration().getName();
        String metaCacheName = ((IgniteEx)g0).igfsx("igfs").configuration().getMetaCacheConfiguration().getName();

        checkIgfsCaches(g0, dataCacheName, metaCacheName);

        Ignite g1 = G.start(config(false, 1));

        checkIgfsCaches(g1, dataCacheName, metaCacheName);

        IgniteFileSystem igfs = g0.fileSystem("igfs");

        igfs.mkdirs(new IgfsPath("/test"));

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(igfs.create(
            new IgfsPath("/test/test.file"), true)))) {

            for (int i = 0; i < 1000; i++)
                bw.write("test-" + i);
        }
    }

    /**
     * @param ignite Ignite.
     * @param dataCacheName Data cache name.
     * @param metaCacheName Meta cache name.
     */
    private void checkIgfsCaches(final Ignite ignite, final String dataCacheName, final String metaCacheName) {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache(dataCacheName);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache(metaCacheName);

                return null;
            }
        }, IllegalStateException.class, null);

        checkCache(((IgniteKernal)ignite).internalCache(dataCacheName));
        checkCache(((IgniteKernal)ignite).internalCache(metaCacheName));
    }

    /**
     * @param cache Cache.
     */
    private void checkCache(GridCacheAdapter cache) {
        assertNotNull(cache);
        assertFalse(cache.context().userCache());
        assertTrue(cache.context().systemTx());
        assertEquals(SYSTEM_POOL, cache.context().ioPolicy());
    }
}
