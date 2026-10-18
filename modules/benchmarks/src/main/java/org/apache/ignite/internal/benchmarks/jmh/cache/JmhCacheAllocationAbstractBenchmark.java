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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.openjdk.jmh.annotations.Param;

/**
 * Base class of the cache allocation benchmarks: two server nodes and, unless the benchmark calls the cache through a
 * server, a client node, all in the benchmark JVM, set up by single-value {@code @Param}s. The nodes keep their data,
 * WAL and change data capture files in a new temporary directory, deleted after the nodes stop, so a persistent run
 * never starts on the data of an earlier one.
 */
public abstract class JmhCacheAllocationAbstractBenchmark extends JmhCacheAbstractBenchmark {
    /** Node that calls the cache. */
    public enum Client {
        /** A server node. */
        SERVER,

        /** A client node. */
        THICK,

        /** A client node with a near cache. */
        NEAR
    }

    /** Where the servers keep the data. */
    public enum Storage {
        /** Memory only. */
        IN_MEMORY,

        /** Ignite Persistence. */
        PERSISTENT
    }

    /** Node that calls the cache. */
    @Param("THICK")
    private Client client;

    /** Atomicity mode. */
    @Param("ATOMIC")
    private CacheAtomicityMode atomicity;

    /** Backups. */
    @Param("1")
    private int backups;

    /** Write synchronization mode. */
    @Param("PRIMARY_SYNC")
    private CacheWriteSynchronizationMode writeSync;

    /** Cache mode. */
    @Param("PARTITIONED")
    private CacheMode cacheMode;

    /** Copy on read. */
    @Param("true")
    private boolean copyOnRead;

    /** On-heap cache. */
    @Param("false")
    private boolean onHeap;

    /** Where the servers keep the data. */
    @Param("IN_MEMORY")
    private Storage storage;

    /** WAL mode, used with persistence or change data capture. */
    @Param("LOG_ONLY")
    private WALMode walMode;

    /** Change data capture. */
    @Param("false")
    private boolean cdc;

    /** Directory of the nodes' data, WAL and change data capture files. */
    private Path storeDir;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        storeDir = Files.createTempDirectory("ignite-jmh-");

        super.setup();

        if (client == Client.NEAR) {
            String name = cache.getName();

            // The base class has started the cache on the client without a near cache.
            cache.close();

            cache = node.getOrCreateNearCache(name, new NearCacheConfiguration<>());
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            super.tearDown();
        }
        finally {
            U.delete(storeDir);
        }
    }

    /**
     * Writes the keys as the base class does, then brings the cache to the state the measured operations should find:
     * with persistence, the servers checkpoint what was written, so the checkpoint doesn't fall into the measurement;
     * with a near cache, every key is read once, so the operations find it in the near cache.
     *
     * @param keys Keys.
     * @param vals Values.
     * @throws Exception If failed.
     */
    @Override protected void populate(Object[] keys, Object[] vals) throws Exception {
        super.populate(keys, vals);

        if (storage == Storage.PERSISTENT) {
            for (Ignite ignite : Ignition.allGrids()) {
                if (!ignite.cluster().localNode().isClient())
                    ((IgniteEx)ignite).context().cache().context().database().waitForCheckpoint("populate");
            }
        }

        if (client == Client.NEAR) {
            for (Object key : keys)
                cache.get(key);
        }
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return client != Client.SERVER;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration configuration(String igniteInstanceName) {
        DataRegionConfiguration region = new DataRegionConfiguration()
            .setPersistenceEnabled(storage == Storage.PERSISTENT)
            .setCdcEnabled(cdc);

        return super.configuration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(storeDir.resolve("db").toString())
                .setWalPath(storeDir.resolve("wal").toString())
                .setWalArchivePath(storeDir.resolve("wal-archive").toString())
                .setCdcWalPath(storeDir.resolve("cdc").toString())
                .setWalMode(walMode)
                .setDefaultDataRegionConfiguration(region));
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration()
            .setAtomicityMode(atomicity)
            .setBackups(backups)
            .setWriteSynchronizationMode(writeSync)
            .setCacheMode(cacheMode)
            .setCopyOnRead(copyOnRead)
            .setOnheapCacheEnabled(onHeap);
    }
}
