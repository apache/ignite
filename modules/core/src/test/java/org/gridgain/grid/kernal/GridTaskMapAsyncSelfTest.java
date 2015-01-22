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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskMapAsyncSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridTaskMapAsyncSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskMap() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        info("Executing sync mapped task.");

        ignite.compute().execute(SyncMappedTask.class, null);

        info("Executing async mapped task.");

        ignite.compute().execute(AsyncMappedTask.class, null);
    }

    /**
     *
     */
    @ComputeTaskMapAsync
    private static class AsyncMappedTask extends BaseTask {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            Collection<? extends ComputeJob> res = super.split(gridSize, arg);

            assert mainThread != mapper;

            return res;
        }
    }

    /**
     *
     */
    private static class SyncMappedTask extends BaseTask {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            Collection<? extends ComputeJob> res = super.split(gridSize, arg);

            assert mainThread == mapper;

            return res;
        }
    }

    /**
     * Test task.
     */
    private abstract static class BaseTask extends ComputeTaskSplitAdapter<Object, Void> {
        /** */
        protected static final Thread mainThread = Thread.currentThread();

        /** */
        protected Thread mapper;

        /** */
        protected Thread runner;

        /** */
        @IgniteLoggerResource
        protected IgniteLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            mapper = Thread.currentThread();

            return Collections.singleton(new ComputeJobAdapter() {
                @Override public Serializable execute() {
                    runner = Thread.currentThread();

                    log.info("Runner: " + runner);
                    log.info("Main: " + mainThread);
                    log.info("Mapper: " + mapper);

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
