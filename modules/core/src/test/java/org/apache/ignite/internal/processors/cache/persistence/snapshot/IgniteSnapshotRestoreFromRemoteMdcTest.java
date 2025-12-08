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
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
    protected static final String CACHE = "cache";

    /** */
    private static final String DC_ID_0 = "DC_ID_0";

    /** */
    private static final String DC_ID_1 = "DC_ID_1";

    /** Cache value builder. */
    private final Function<Integer, Object> valBuilder = String::valueOf;

    /** */
    protected ListeningTestLogger listeningLog = new ListeningTestLogger(log);

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
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
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
    public void testMdcAwareSnapshot() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        Ignite supplier = startGridWithCustomWorkdir("supplier");

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx other = startGridWithCustomWorkdir("other_dc_node");

        fillCache(other);

        forceCheckpoint();

        snp(other).createSnapshot(SNAPSHOT_NAME, null, false, false).get(TIMEOUT);

        other.cache(CACHE).destroy();

        awaitPartitionMapExchange();

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        startGridWithCustomWorkdir("demander");

        LogListener supLsnr = LogListener.matches("Getting partition from remote node [node=" +
            supplier.cluster().localNode().id()).build();
        LogListener otherLsnr = LogListener.matches("Getting partition from remote node [node=" +
            other.cluster().localNode().id()).build();

        listeningLog.registerListener(supLsnr);
        listeningLog.registerListener(otherLsnr);

        other.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE)).get(TIMEOUT);

        assertTrue(supLsnr.check());
        assertFalse(otherLsnr.check());

        assertCacheKeys(other.cache(CACHE), CACHE_KEYS_RANGE);
    }

    /** */
    private void fillCache(IgniteEx ignite) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE);
        ccfg.setBackups(2); // To fill both nodes

        ignite.createCache(ccfg);

        try (IgniteDataStreamer<Integer, Object> ds = ignite.dataStreamer(ccfg.getName())) {
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ds.addData(i, valBuilder.apply(i));
        }
    }

    /** */
    private IgniteEx startGridWithCustomWorkdir(String instanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(instanceName);

        cfg.setWorkDirectory(Paths.get(defaultWorkDirectory(), U.maskForFileName(instanceName)).toString());

        IgniteEx ignite = startGrid(cfg);

        ignite.cluster().state(ClusterState.ACTIVE);
        resetBaselineTopology();
        awaitPartitionMapExchange();

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
