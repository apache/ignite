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

package org.apache.ignite.internal.processors.security.cluster;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SUCCESS_FILE;
import static org.apache.ignite.internal.processors.security.cluster.ClusterNodeOperationPermissionTest.Operation.RESTART;
import static org.apache.ignite.internal.processors.security.cluster.ClusterNodeOperationPermissionTest.Operation.RESTART_ALL;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_NODE_STOP;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClusterNodeOperationPermissionTest extends AbstractSecurityTest {
    /** */
    private IgniteClient cli;

    /** */
    private IgniteProcessProxy ignite;

    /** */
    public static final String RESTART_FILE_PATH = "src/test/resources/ignite-restart.tmp";

    /** */
    private IgniteConfiguration configuration(int idx) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        return getConfiguration(
            login,
            new TestSecurityPluginProvider(
                login,
                "",
                 systemPermissions(JOIN_AS_SERVER),
                null,
                false,
                new TestSecurityData("stop-allowed", clientPermissionsBuilder()
                    .appendSystemPermissions(ADMIN_CLUSTER_NODE_STOP)
                    .build()),
                new TestSecurityData("stop-forbidden", clientPermissionsBuilder().build())
            ))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(1)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testStartNodeAuthorization() throws Exception {
        IgniteEx ignite = startGrid(configuration(0));

        assertAuthorizationFailed(
            () -> ignite.cluster().startNodes(new File("src/test/config/start-nodes.ini"), true, 1, 1),
            SecurityException.class
        );

        assertAuthorizationFailed(() ->
            ignite.cluster().startNodesAsync(new File("src/test/config/start-nodes.ini"), true, 1, 1).get(),
            SecurityException.class
        );

        assertAuthorizationFailed(() ->
            ignite.cluster().startNodes(Collections.emptyList(), Collections.emptyMap(), true, 1, 1),
            SecurityException.class
        );

        assertAuthorizationFailed(() ->
            ignite.cluster().startNodes(Collections.emptyList(), Collections.emptyMap(), true, 1, 1),
            SecurityException.class
        );
    }

    /** */
    @Test
    public void testStopNodeAuthorization() throws Exception {
        for (Operation op : Operation.values()) {
            doClusterNodeTest("stop-allowed", op, true);
            doClusterNodeTest("stop-forbidden", op, false);
        }
    }

    /**
     * We are forced to initiate the test operation through the thin client, because otherwise the node stop/restart
     * operations will halt the local test JVM.
     */
    private void doClusterNodeTest(String login, Operation op, boolean isSuccessExpected) throws Exception {
        prepareCluster(login);

        try {
            if (isSuccessExpected) {
                try {
                    cli.compute().execute(ClusterNodeOperationExecutor.class.getName(), op);
                }
                catch (ClientException e) {
                    // It is possible for the Ignite node to shut down before the task execution response is sent.
                    assertTrue(e.getMessage().contains("Task cancelled due to stopping of the grid"));
                }

                assertTrue(waitForCondition(() -> !ignite.getProcess().getProcess().isAlive(), getTestTimeout()));

                assertTrue((op != RESTART && op != RESTART_ALL) || new File(RESTART_FILE_PATH).exists());
            }
            else {
                assertAuthorizationFailed(
                    () -> cli.compute().execute(ClusterNodeOperationExecutor.class.getName(), op),
                    ClientException.class
                );

                assertTrue(ignite.getProcess().getProcess().isAlive());
            }
        }
        finally {
            stopCluster();
        }
    }

    /** */
    private void prepareCluster(String opExecutorLogin) throws Exception {
        ignite = new IgniteProcessProxy(
            configuration(0),
            log,
            null,
            true,
            singletonList("-D" + IGNITE_SUCCESS_FILE + "=" + RESTART_FILE_PATH)
        );

        AtomicReference<IgniteClient> cli = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            try {
                cli.set(startClient(opExecutorLogin));

                return true;
            }
            catch (ClientConnectionException e) {
                return false;
            }
        }, getTestTimeout()));

        this.cli = cli.get();
    }

    /** */
    private void stopCluster() throws Exception {
        cli.close();

        ignite.kill();

        waitForCondition(() -> !ignite.getProcess().getProcess().isAlive(), getTestTimeout());

        File restartFile = new File(RESTART_FILE_PATH);

        if (restartFile.exists())
            restartFile.delete();
    }

    /** */
    private void assertAuthorizationFailed(RunnableX r, Class<? extends Exception> exCls) {
        assertThrowsAnyCause(
            log,
            () -> {
                r.run();

                return null;
            },
            exCls,
            "Authorization failed"
        );
    }

    /** */
    private IgniteClient startClient(String login) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setUserName(login)
            .setUserPassword(""));
    }

    /** */
    private SecurityPermissionSetBuilder clientPermissionsBuilder() {
        return new SecurityPermissionSetBuilder()
            .defaultAllowAll(false)
            .appendTaskPermissions(ClusterNodeOperationExecutor.class.getName(), TASK_EXECUTE);
    }

    /** */
    public static final class ClusterNodeOperationExecutor extends ComputeTaskAdapter<Operation, Void> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Operation arg
        ) throws IgniteException {
            return Collections.singletonMap(new Job(arg), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }

        /** */
        private static final class Job extends ComputeJobAdapter {
            /** */
            private final Operation op;

            /** */
            @IgniteInstanceResource
            Ignite ignite;

            /** */
            public Job(Operation op) {
                this.op = op;
            }

            /** {@inheritDoc} */
            @Override public Object execute() throws IgniteException {
                switch (op) {
                    case STOP: ignite.cluster().stopNodes(singleton(ignite.cluster().localNode().id())); break;
                    case STOP_ALL: ignite.cluster().stopNodes(); break;
                    case RESTART: ignite.cluster().restartNodes(singleton(ignite.cluster().localNode().id())); break;
                    case RESTART_ALL: ignite.cluster().restartNodes(); break;
                }

                return null;
            }
        }
    }

    /** */
    public enum Operation {
        /** */
        STOP,

        /** */
        STOP_ALL,

        /** */
        RESTART,

        /** */
        RESTART_ALL
    }
}
