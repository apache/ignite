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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.processedPartitionComparator;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.toStringTopProcessingPartitions;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.trimToSize;

/**
 * Class for testing the restoration of the status of partitions.
 */
public class RestorePartitionStateTest extends GridCommonAbstractTest {
    /** Timeout for displaying the progress of restoring the status of partitions. */
    private long timeoutOutputRestoreProgress;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();

        timeoutOutputRestoreProgress = TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();

        TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS = timeoutOutputRestoreProgress;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "0")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "1")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "3")
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /**
     * Checking the correctness of the string representation of the top.
     */
    @Test
    public void testTopRestorePartitionsToString() {
        TreeSet<T3<Long, Long, GroupPartitionId>> s = new TreeSet<>(processedPartitionComparator());

        s.add(new T3<>(10L, 0L, new GroupPartitionId(0, 0)));
        s.add(new T3<>(10L, 0L, new GroupPartitionId(0, 1)));
        s.add(new T3<>(10L, 0L, new GroupPartitionId(1, 1)));

        s.add(new T3<>(20L, 0L, new GroupPartitionId(2, 0)));
        s.add(new T3<>(20L, 0L, new GroupPartitionId(2, 1)));

        String exp = "[[time=20ms [[grp=2, part=[0, 1]]]], [time=10ms [[grp=0, part=[0, 1]], [grp=1, part=[1]]]]]";

        assertEquals(exp, toStringTopProcessingPartitions(s, Collections.emptyList()));
    }

    /**
     * Testing Method {@link GridCacheProcessor#trimToSize}.
     */
    @Test
    public void testTrimToSize() {
        TreeSet<Long> act = new TreeSet<>();

        trimToSize(act, 0);
        trimToSize(act, 1);

        LongStream.range(0, 10).forEach(act::add);

        trimToSize(act, act.size());
        assertEquals(10, act.size());

        TreeSet<Long> exp = new TreeSet<>(act);

        trimToSize(act, 9);
        assertEqualsCollections(exp.tailSet(1L), act);

        trimToSize(act, 5);
        assertEqualsCollections(exp.tailSet(5L), act);

        trimToSize(act, 3);
        assertEqualsCollections(exp.tailSet(7L), act);

        trimToSize(act, 0);
        assertEqualsCollections(exp.tailSet(10L), act);
    }

    /**
     * Testing Method {@link GridCacheProcessor#processedPartitionComparator}.
     */
    @Test
    public void testProcessedPartitionComparator() {
        List<T3<Long, Long, GroupPartitionId>> exp = F.asList(
            new T3<>(0L, 2L, new GroupPartitionId(0, 0)),
            new T3<>(0L, 1L, new GroupPartitionId(0, 0)),
            new T3<>(1L, 1L, new GroupPartitionId(0, 0)),
            new T3<>(1L, 0L, new GroupPartitionId(0, 0)),
            new T3<>(1L, 0L, new GroupPartitionId(1, 0)),
            new T3<>(1L, 0L, new GroupPartitionId(1, 1))
        );

        TreeSet<T3<Long, Long, GroupPartitionId>> act = new TreeSet<>(processedPartitionComparator());
        act.addAll(exp);

        assertEqualsCollections(exp, act);
    }

    /**
     * Check that the progress of restoring partitions with the top partitions is displayed in the log.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testLogTopPartitions() throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        ((GridCacheDatabaseSharedManager)n.context().cache().context().database())
            .enableCheckpoints(false).get(getTestTimeout());

        for (IgniteInternalCache cache : n.context().cache().caches()) {
            for (int i = 0; i < 1_000; i++)
                cache.put(i, cache.name() + i);
        }

        stopAllGrids();
        awaitPartitionMapExchange();

        LogListener startPartRestore = LogListener.matches(logStr -> {
            if (logStr.startsWith("Started restoring partition state [grp=")) {
                try {
                    U.sleep(15);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }

                return true;
            }

            return false;
        }).build();

        LogListener progressPartRestore = LogListener.matches("Restore partitions state progress")
            .andMatches("topProcessedPartitions").build();

        LogListener finishPartRestore = LogListener.matches("Finished restoring partition state for local groups")
            .andMatches("topProcessedPartitions").build();

        TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS = 150L;

        setLoggerDebugLevel();

        startGrid(0, (UnaryOperator<IgniteConfiguration>)cfg -> {
            cfg.setGridLogger(new ListeningTestLogger(log, startPartRestore, progressPartRestore, finishPartRestore));

            return cfg;
        });

        assertTrue(startPartRestore.check());
        assertTrue(progressPartRestore.check());
        assertTrue(finishPartRestore.check());
    }
}
