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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Strings;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CHECKPOINT_THREADS;

/**
 * Puts data into grid, waits for checkpoint to start and then verifies data
 */
public class IgnitePdsCheckpointSimpleTest extends GridCommonAbstractTest {
    /** Checkpoint threads. */
    public int cpThreads = DFLT_CHECKPOINT_THREADS;

    /** Checkpoint period. */
    public long cpFrequency = TimeUnit.SECONDS.toMillis(10);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(cpFrequency));

        if (cpThreads != DFLT_CHECKPOINT_THREADS) {
            cfg.getDataStorageConfiguration()
                .setCheckpointThreads(cpThreads);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
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
    public void testStartNodeWithDefaultCpThreads() throws Exception {
        checkCheckpointThreads();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodeWithNonDefaultCpThreads() throws Exception {
        cpThreads = 10;

        checkCheckpointThreads();
    }

    /**
     * Checks that all checkpoints in the different node spaced apart of enough amount of time.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStartCheckpointDelay() throws Exception {
        cpFrequency = TimeUnit.SECONDS.toMillis(2);

        int nodes = 4;

        IgniteEx ignite0 = startGrids(nodes);

        ignite0.cluster().state(ClusterState.ACTIVE);

        SimpleDistributedProperty<Integer> cpFreqDeviation = U.field(((IgniteEx)ignite0).context().cache().context().database(),
            "cpFreqDeviation");

        cpFreqDeviation.propagate(25);

        doSleep(cpFrequency * 2);

        TreeSet<Long> cpStartTimes = new TreeSet<>();

        for (int i = 0; i < nodes; i++)
            cpStartTimes.add(getLatCheckpointStartTime(ignite(i)));

        assertEquals(nodes, cpStartTimes.size());

        Long prev = 0L;

        for (Long st : cpStartTimes) {
            assertTrue("There was nothing checkpoint in this node yet.", st != 0);

            assertTrue("Checkpoint started so close on different nodes [one=" + prev + ", other=" + st + ']',
                st - prev > 200);
        }
    }

    /**
     * Gets a timestamp where latest checkpoint occurred.
     *
     * @param ignite Ignite.
     * @return Latest checkpoint start time.
     */
    private Long getLatCheckpointStartTime(IgniteEx ignite) {
        return U.field(((GridCacheDatabaseSharedManager)ignite.context().cache().context().database()).getCheckpointer(),
            "lastCpTs");
    }

    /**
     * Checks that all checkpoint threads are present in JVM.
     *
     * @throws Exception If failed.
     */
    public void checkCheckpointThreads() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("cache");

        cache.put(1, 1);

        forceCheckpoint();

        int dbCpThread = 0, ioCpRunner = 0, cpuCpRunner = 0;

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("db-checkpoint-thread"))
                dbCpThread++;

            else if (t.getName().contains("checkpoint-runner-IO"))
                ioCpRunner++;

            else if (t.getName().contains("checkpoint-runner-cpu"))
                cpuCpRunner++;
        }

        assertEquals(1, dbCpThread);
        assertEquals(cpThreads, ioCpRunner);
        assertEquals(cpThreads, cpuCpRunner);
    }

    /**
     * Checks if same data can be loaded after checkpoint.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testRecoveryAfterCpEnd() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("cache");

        for (int i = 0; i < 10000; i++)
            cache.put(i, valueWithRedundancyForKey(i));

        ignite.context().cache().context().database().waitForCheckpoint("test");

        stopAllGrids();

        IgniteEx igniteRestart = startGrid(0);

        igniteRestart.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cacheRestart = igniteRestart.getOrCreateCache("cache");

        for (int i = 0; i < 10000; i++)
            assertEquals(valueWithRedundancyForKey(i), cacheRestart.get(i));

        stopAllGrids();
    }

    /**
     * @param i key.
     * @return value with extra data, which allows to verify
     */
    private @NotNull String valueWithRedundancyForKey(int i) {
        return Strings.repeat(Integer.toString(i), 10);
    }
}
