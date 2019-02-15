/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class GridTaskMapAsyncSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridTaskMapAsyncSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTaskMap() throws Exception {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
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
        @LoggerResource
        protected IgniteLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
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
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
