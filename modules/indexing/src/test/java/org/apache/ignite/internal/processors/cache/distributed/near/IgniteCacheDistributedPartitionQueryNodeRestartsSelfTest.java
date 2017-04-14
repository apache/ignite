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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.jsr166.ThreadLocalRandom8;

/**
 * Tests distributed queries over set of partitions.
 *
 * The test assigns partition ranges to specific grid nodes using special affinity implementation.
 */
public class IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_COUNT = 10;

    /** Restarting grids count. */
    private static final int RESTARTING_GRIDS_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRIDS_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** */
    protected void doRestarts() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        final GridRandom r = new GridRandom();

        final AtomicIntegerArray states = new AtomicIntegerArray(GRIDS_COUNT);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while(!stop.get()) {
                    int grid = r.nextInt(GRIDS_COUNT);

                    if (states.compareAndSet(grid, 0, 1)) {
                        stopGrid(grid);

                        Thread.sleep(r.nextInt(3_000) + 1_000);

                        startGrid(grid);

                        states.set(grid, 1);
                    }
                }

                return null;
            }
        }, RESTARTING_GRIDS_COUNT);

        grid(0).scheduler().runLocal(new Runnable() {
            @Override public void run() {
                stop.set(true);
            }
        }, 10, TimeUnit.SECONDS);

        fut.get();
    }

    /** */
    public void testRestarts() throws Exception {
        doRestarts();
    }
}