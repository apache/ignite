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
package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.util.Collection;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/** */
public class IgniteCacheDumpSeveralDiskTest extends GridCommonAbstractTest {
    /** */
    public static final String NVME_1 = "nvme1";

    /** */
    public static final String NVME_2 = "nvme2";

    /** */
    public static final String NVME_4 = "nvme4";

    /** */
    public static final String NVME_5 = "nvme5";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
        U.delete(new File(U.defaultWorkDirectory()));

        assertTrue(U.mkdirs(new File(path(NVME_5))));

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> c = srv.createCache(new CacheConfiguration<>("cache")
            .setStoragePaths(path(NVME_1), path(NVME_2), path(NVME_4))
            .setAffinity(new RendezvousAffinityFunction().setPartitions(3)));

        IntStream.range(0, 10).forEach(i -> c.put(i, i));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg
            .setStoragePath(path(NVME_1))
            .setExtraStoragePaths(path(NVME_2), path(NVME_4))
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        return cfg.setDataStorageConfiguration(dsCfg).setSnapshotPath("/dev/null");
    }

    /** */
    @Test
    public void testSeveralDisksDump() throws Exception {
        createSnapshot(true);
    }

    /** */
    @Test
    public void testSeveralDisksSnapshot() throws Exception {
        createSnapshot(false);
    }

    private void createSnapshot(boolean dump) throws Exception {
        String name = dump ? "dump" : "snapshot";
        String snpPath = path("nvme5/snapshot");
        Collection<String> cacheGrpNames = null;
        boolean incremental = false;
        boolean onlyPrimary = true;
        boolean compress = false;
        boolean encrypt = false;

        grid(0).context().cache().context().snapshotMgr().createSnapshot(
            name,
            snpPath,
            cacheGrpNames,
            incremental,
            onlyPrimary,
            dump,
            compress,
            encrypt,
            false,
            false
        ).get();
    }

    /** */
    private static @NotNull String path(String path) throws IgniteCheckedException {
        return U.defaultWorkDirectory() + "/" + path;
    }
}
