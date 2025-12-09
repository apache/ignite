/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.defaultWorkDirectory;

/** */
public class IgniteSnapshotRestoreFromRemoteMdcTest extends AbstractSnapshotSelfTest {
    /** Cache. */
    private static final String CACHE = "cache";

    /** */
    private static final String DC_ID_0 = "DC_ID_0";

    /** */
    private static final String DC_ID_1 = "DC_ID_1";

    /** */
    private ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** @throws Exception If fails. */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
        cleanupDedicatedPersistenceDirs();
    }

    /** @throws Exception If fails. */
    @After
    public void after() throws Exception {
        afterTestSnapshot();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testMdcAwareSnapshotFromCurrentDc() throws Exception {
        testMdcAwareSnapshot(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testMdcAwareSnapshotFromAnyDc() throws Exception {
        testMdcAwareSnapshot(false);
    }

    /** @throws Exception If failed. */
    private void testMdcAwareSnapshot(boolean replicatedCache) throws Exception {
        Ignite supplier = startGridWithCustomWorkdir("supplier", DC_ID_0);

        IgniteEx other = startGridWithCustomWorkdir("other_dc_node", DC_ID_1);

        other.cluster().state(ClusterState.ACTIVE);

        fillCache(other, replicatedCache);

        snp(other).createSnapshot(SNAPSHOT_NAME, null, false, false).get(TIMEOUT);

        other.cache(CACHE).destroy();

        awaitPartitionMapExchange();

        startGridWithCustomWorkdir("demander", DC_ID_0);

        resetBaselineTopology();

        LogListener supLsnr = LogListener.matches(" sec, rmtId=" + supplier.cluster().localNode().id()).build();
        LogListener otherLsnr = LogListener.matches(" sec, rmtId=" + other.cluster().localNode().id()).build();

        listeningLog.registerListener(supLsnr);
        listeningLog.registerListener(otherLsnr);

        other.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE)).get(60_000);

        assertTrue(supLsnr.check());
        assertEquals(!replicatedCache, otherLsnr.check());

        assertCacheKeys(other.cache(CACHE), CACHE_KEYS_RANGE);
    }

    /** */
    private void fillCache(IgniteEx ignite, boolean replicatedCache) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE);

        if (replicatedCache)
            ccfg.setCacheMode(CacheMode.REPLICATED); // Fill all nodes with partitions.

            ignite.createCache(ccfg);

        try (IgniteDataStreamer<Integer, Object> ds = ignite.dataStreamer(ccfg.getName())) {
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ds.addData(i, valueBuilder().apply(i));
        }
    }

    /** */
    private IgniteEx startGridWithCustomWorkdir(String instanceName, String dcId) throws Exception {
        IgniteConfiguration cfg = getConfiguration(instanceName)
            .setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId));

        cfg.setWorkDirectory(Paths.get(defaultWorkDirectory(), U.maskForFileName(instanceName)).toString());

        IgniteEx ignite = startGrid(cfg);

        return ignite;
    }

    /** */
    protected static void cleanupDedicatedPersistenceDirs() {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Path.of(defaultWorkDirectory()))) {
            for (Path dir : ds)
                U.delete(dir);
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
