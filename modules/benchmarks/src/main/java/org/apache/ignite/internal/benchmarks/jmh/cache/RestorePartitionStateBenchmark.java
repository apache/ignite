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
package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class RestorePartitionStateBenchmark {
    /** */
    private IgniteConfiguration getConfiguration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** */
    public void benchmark() throws Exception {
        cleanPersistenceDir();

        System.setProperty(IGNITE_QUIET, "false");

        Ignite ignite = Ignition.start(getConfiguration("ignite"));

        ignite.cluster().state(ClusterState.ACTIVE);

        List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();

        for (int i = 1; i < 50; i++) {
            int parts;

            if (i < 20)
                parts = 10;
            else if (i < 40)
                parts = 100;
            else if (i < 45)
                parts = 200;
            else if (i < 49)
                parts = 500;
            else
                parts = 4_000;

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
                new CacheConfiguration<Integer, Integer>("cache" + i)
                    .setAffinity(new RendezvousAffinityFunction(false, parts))
            );

            System.out.println("working on cache: " + cache.getName());

            for (int j = 0; j < parts; j++)
                ignite.dataStreamer(cache.getName()).addData(j, j);

            ignite.dataStreamer(cache.getName()).flush();

            caches.add(cache);
        }

        GridCacheDatabaseSharedManager dbMgr =
            (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context().cache().context().database();

        dbMgr.wakeupForCheckpoint("test").get();

        Ignition.stop(ignite.name(), false);

        // See 'Finished restoring partition state for local groups ...' log message to know how much time
        // has restore taken.
        ignite = Ignition.start(getConfiguration("ignite"));

        Thread.sleep(1000);

        Ignition.stop(ignite.name(), false);

        cleanPersistenceDir();

        System.clearProperty(IGNITE_QUIET);
    }

    /**
     * Cleans persistent directory.
     *
     * @throws Exception if failed.
     */
    private void cleanPersistenceDir() throws Exception {
        if (!F.isEmpty(G.allGrids()))
            throw new IgniteException("Grids are not stopped");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** */
    public static void main(String[] args) throws Exception {
        new RestorePartitionStateBenchmark().benchmark();
    }
}
