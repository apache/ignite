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
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.openjdk.jmh.annotations.Param;

/** Base of the cache allocation benchmarks: two server nodes and, unless {@code client=SERVER}, a client node. */
public abstract class JmhCacheAllocationAbstractBenchmark extends JmhCacheAbstractBenchmark {
    /** Entry processor of the {@code invoke} benchmarks. */
    protected static final CacheEntryProcessor<Object, Object, Object> SAME_VALUE = new SameValueProcessor();

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

    /** Keys the setup writes. */
    protected Object[] keys;

    /** Values of {@link #keys}. */
    protected Object[] vals;

    /** Nodes' data, WAL and CDC files: a new temporary directory per run, so a persistent run starts empty. */
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

    /** Writes an {@link IntValue} of each key from 0 to {@code CNT - 1}, as {@link #keys} and {@link #vals}. */
    protected void populateIntValues() throws Exception {
        keys = new Integer[CNT];
        vals = new IntValue[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = new IntValue(i);
        }

        populate(keys, vals);
    }

    /** Writes the keys, then checkpoints them with persistence and reads each into the near cache if there is one. */
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
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(backups)
            .setWriteSynchronizationMode(writeSync)
            .setCacheMode(cacheMode)
            .setCopyOnRead(copyOnRead)
            .setOnheapCacheEnabled(onHeap);
    }

    /** Writes the entry's value back, so the entry changes as with a put of the same value. */
    private static class SameValueProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
            entry.setValue(entry.getValue());

            return null;
        }
    }
}
