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

package org.apache.ignite.internal.processors.security;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;

/**
 * Unit test for {@link IgniteSecurityProcessor}.
 */
public class IgniteSecurityProcessorTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch taskExecutionStartedLatch;

    /** */
    private static CountDownLatch taskExecutionUnblockedLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setThinClientConfiguration(new ThinClientConfiguration()
                .setMaxActiveComputeTasksPerConnection(1)));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** Tests scenario when {@link SecurityContext} cannot be obtained on remote node. */
    @Test
    public void testUserDropDuringOperation() throws Exception {
        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        String user = "cli";
        String pwd = "pwd";

        try (AutoCloseable ignored = withSecurityContextOnAllNodes(authenticate(grid(0), "ignite", "ignite"))) {
            grid(0).context().security().createUser(user, pwd.toCharArray());
        }

        taskExecutionStartedLatch = new CountDownLatch(1);
        taskExecutionUnblockedLatch = new CountDownLatch(1);

        try (
            IgniteClient cli = Ignition.startClient(new ClientConfiguration()
                .setAddresses("127.0.0.1:10800")
                .setUserName(user)
                .setUserPassword(pwd))
        ) {
            IgniteClientFuture<Void> fut = cli.compute().executeAsync2(TestTask.class.getName(), grid(1).localNode().id());

            taskExecutionStartedLatch.await();

            try (AutoCloseable ignored = withSecurityContextOnAllNodes(authenticate(grid(0), "ignite", "ignite"))) {
                grid(0).context().security().dropUser(user);
            }

            taskExecutionUnblockedLatch.countDown();

            GridTestUtils.assertThrowsAnyCause(
                log, () -> fut.get(getTestTimeout(), MILLISECONDS),
                ExecutionException.class,
                "Failed to obtain user security context. The user has probably been deleted in the middle of Ignite" +
                    " operation execution"
            );
        }
    }

    /** Test task that performs default cache put on the node with id that passed via task argument. */
    public static class TestTask extends ComputeTaskAdapter<UUID, Void> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable UUID nodeId
        ) throws IgniteException {
            taskExecutionStartedLatch.countDown();

            try {
                taskExecutionUnblockedLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            for (ClusterNode node : subgrid) {
                if (node.id().equals(nodeId)) {
                    return Collections.singletonMap(new ComputeJobAdapter() {
                        @IgniteInstanceResource
                        private Ignite ignite;

                        @Override public void cancel() {
                            // No-op.
                        }

                        @Override public Serializable execute() {
                            ignite.cache(DEFAULT_CACHE_NAME).put("key", "val");

                            return null;
                        }
                    }, node);
                }
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
