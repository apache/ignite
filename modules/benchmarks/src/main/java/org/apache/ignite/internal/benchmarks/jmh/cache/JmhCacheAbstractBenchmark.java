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

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Base class for cache benchmarks.
 */
@State(Scope.Benchmark)
public class JmhCacheAbstractBenchmark extends JmhAbstractBenchmark {
    /** Property: backups. */
    protected static final String PROP_BACKUPS = "ignite.jmh.cache.backups";

    /** Property: atomicity mode. */
    protected static final String PROP_ATOMICITY_MODE = "ignite.jmh.cache.atomicityMode";

    /** Property: atomicity mode. */
    protected static final String PROP_WRITE_SYNC_MODE = "ignite.jmh.cache.writeSynchronizationMode";

    /**
     * Setup routine. Child classes must invoke this method first.
     *
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
        System.out.println();
        System.out.println("--------------------");

        System.out.println("IGNITE BENCHMARK INFO: ");

        System.out.println("\tbackups:                    " + intProperty(PROP_BACKUPS));

        System.out.println("\tatomicity mode:             " +
            enumProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.class));

        System.out.println("\twrite synchronization mode: " +
            enumProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.class));

        System.out.println("--------------------");
        System.out.println();
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        Ignition.stopAll(true);
    }

    /**
     * Create cache configuration.
     *
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(null);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        // Set atomicity mode.
        CacheAtomicityMode atomicityMode = enumProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.class);

        if (atomicityMode != null)
            cacheCfg.setAtomicityMode(atomicityMode);

        // Set write synchronization mode.
        CacheWriteSynchronizationMode writeSyncMode =
            enumProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.class);

        if (writeSyncMode != null)
            cacheCfg.setWriteSynchronizationMode(writeSyncMode);

        // Set backups.
        cacheCfg.setBackups(intProperty(PROP_BACKUPS));

        return cacheCfg;
    }
}
