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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.TableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

/**
 * Tests partition reservation/releasing for queries over unstable topology.
 */
public class PartitionsReservationIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int PARTS = 16;

    /** */
    private static final int KEYS = PARTS * 100_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. Don't start any grids.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);

        client = startClientGrid();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, Employer.class)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        return cfg;
    }

    /** */
    @Test
    public void testIndexCount() throws Exception {
        checkPartitionsReservationRelease(
            assertQuery("SELECT COUNT(_key) FROM Employer")
                .matches(QueryChecker.containsSubPlan(IgniteIndexCount.class.getSimpleName()))
                .ordered() // To avoid modification of QueryChecker.
                .returns((long)KEYS));
    }

    /** */
    @Test
    public void testIndexScan() throws Exception {
        checkPartitionsReservationRelease(
            assertQuery("SELECT /*+ FORCE_INDEX */ * FROM Employer")
                .matches(QueryChecker.containsSubPlan(IndexScan.class.getSimpleName()))
                .resultSize(KEYS));
    }

    /** */
    @Test
    public void testTableScan() throws Exception {
        checkPartitionsReservationRelease(
            assertQuery("SELECT * FROM Employer")
                .matches(QueryChecker.containsSubPlan(TableScan.class.getSimpleName()))
                .resultSize(KEYS));
    }

    /** */
    public void checkPartitionsReservationRelease(QueryChecker checker) throws Exception {
        try (IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < KEYS; i++)
                streamer.addData(i, new Employer("name" + i, (double)i));
        }

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> qryFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    try {
                        checker.check();
                    }
                    catch (IgniteSQLException e) {
                        if (X.hasCause(e, ClusterTopologyException.class))
                            continue; // Expected when topology changed while query starts.

                        throw e;
                    }
                }
            }
        }, 10, "qry-thread");

        // Trigger rebalance and partitions eviction.
        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange();

        doSleep(1_000L);

        stop.set(true);

        qryFut.get();
    }
}
