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

import java.security.Permissions;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.util.lang.GridFunc.asList;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/**
 * Tests permissions of cluster state change.
 */
@RunWith(Parameterized.class)
public class ClusterStatePermissionTest extends AbstractSecurityTest {
    /** */
    private static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** */
    private SecurityPermission[] permissions = EMPTY_PERMS;

    /** From-client flag parameter. */
    @Parameterized.Parameter
    public NodeType operationNodeType;

    /** Persistence flag parameter. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "nodeType={0}, persistence={1}")
    public static Collection<?> parameters() {
        return cartesianProduct(asList(NodeType.values()), asList(false, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));
        }

        SecurityPermission[] srvPerms = F.asArray(JOIN_AS_SERVER);
        SecurityPermission[] clientPerms = EMPTY_PERMS;

        if (operationNodeType == NodeType.SERVER)
            srvPerms = F.concat(srvPerms, permissions);
        else if (operationNodeType == NodeType.CLIENT)
            srvPerms = permissions;
        else if (operationNodeType == NodeType.THIN_CLIENT)
            clientPerms = permissions;
        else if (operationNodeType == NodeType.GRID_CLIENT) {
            clientPerms = permissions;

            cfg.setConnectorConfiguration(new ConnectorConfiguration());
        }

        SecurityPermissionSetBuilder secBuilder = create().defaultAllowAll(false)
            .appendSystemPermissions(F.concat(srvPerms, JOIN_AS_SERVER));

        TestSecurityPluginProvider secPlugin = new TestSecurityPluginProvider(
            instanceName,
            "",
            secBuilder.build(),
            false,
            new TestSecurityData(
                "client",
                "",
                create().defaultAllowAll(false).appendSystemPermissions(clientPerms).build(),
                new Permissions()
            )
        );

        cfg.setPluginProviders(secPlugin);

        cfg.setClusterStateOnStart(INACTIVE);

        return cfg;
    }

    /**
     * Tests both activation and deactivation are allowed.
     */
    @Test
    public void testActivationDeactivationAllowed() throws Exception {
        permissions = F.asArray(ADMIN_CLUSTER_STATE);

        doTestChangeState(
            F.asArray(ACTIVE_READ_ONLY, INACTIVE, ACTIVE_READ_ONLY, ACTIVE, INACTIVE, ACTIVE, ACTIVE_READ_ONLY),
            F.asArray(TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE)
        );
    }

    /**
     * Tests activation is not allowed.
     */
    @Test
    public void testActivationNotAllowed() throws Exception {
        doTestChangeState(F.asArray(ACTIVE), F.asArray(FALSE));
    }

    /**
     * Tests activation is allowed, deactivation is not allowed.
     */
    @Test
    public void testDeactivationNotAllowed() throws Exception {
        startAllAllowedNode();

        doTestChangeState(F.asArray(INACTIVE), F.asArray(FALSE));
    }

    /**
     * Tests attempt to set same state is allowed without the permission.
     */
    @Test
    public void testSameStateNotAllowedWithoutPermission() throws Exception {
        startAllAllowedNode();

        doTestChangeState(F.asArray(ACTIVE), F.asArray(FALSE));
    }

    /**
     * Starts server node, activates it and restores the test configuration.
     */
    private void startAllAllowedNode() throws Exception {
        NodeType nodeType = operationNodeType;
        SecurityPermission[] perms = permissions;

        operationNodeType = NodeType.SERVER;
        permissions = F.asArray(ADMIN_CLUSTER_STATE);

        Ignite ig = startGrids(1);

        ig.cluster().state(ACTIVE);

        assertEquals(ACTIVE, ig.cluster().state());

        operationNodeType = nodeType;
        permissions = perms;
    }

    /**
     * Tries to change state and ensures a proper error raises if required.
     */
    private void doTestChangeState(ClusterState[] states, Boolean[] allowed) throws Exception {
        assert states != null && allowed != null && states.length == allowed.length && states.length > 0;

        Ignite ig = startGrid(G.allGrids().size());

        startGrid(G.allGrids().size());

        Consumer<ClusterState> action = nodeStateAction(ig);

        for (int i = 0; i < states.length; ++i) {
            assert allowed[i] != null;

            ClusterState stateTo = states[i];

            if (allowed[i]) {
                action.accept(stateTo);

                assertEquals(stateTo, ig.cluster().state());
            }
            else
                ensureThrows(action, stateTo);
        }
    }

    /**
     * Ensures that a proper error occurs.
     */
    private void ensureThrows(Consumer<ClusterState> action, ClusterState stateTo) {
        Class<? extends Throwable> cause = SecurityException.class;
        String errMsg = "Authorization failed [perm=" + ADMIN_CLUSTER_STATE;

        if (NodeType.THIN_CLIENT == operationNodeType) {
            cause = ClientAuthorizationException.class;
            errMsg = "User is not authorized to perform this operation";
        }
        else if (NodeType.GRID_CLIENT == operationNodeType)
            cause = GridClientException.class;

        assertThrowsAnyCause(
            null,
            () -> {
                action.accept(stateTo);

                return null;
            },
            cause,
            errMsg
        );
    }

    /**
     * @return Change state operation depending on {@link #operationNodeType}.
     */
    private Consumer<ClusterState> nodeStateAction(Ignite srv) throws Exception {
        switch (operationNodeType) {
            case SERVER:
                return (state) -> srv.cluster().state(state);

            case CLIENT:
                return (state) -> {
                    try {
                        startClientGrid(G.allGrids().size()).cluster().state(state);
                    }
                    catch (Exception e) {
                        throw new IgniteException("Unable to start client grid.", e);
                    }
                };

            case THIN_CLIENT: {
                return (state) -> G.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                    .setUserName("client").setUserPassword("")).cluster().state(state);
            }

            case GRID_CLIENT: {
                GridClientConfiguration cfg = new GridClientConfiguration();

                cfg.setServers(asList("127.0.0.1:11211"));

                cfg.setSecurityCredentialsProvider(
                    new SecurityCredentialsBasicProvider(new SecurityCredentials("client", "")));

                GridClient gridCleint = GridClientFactory.start(cfg);

                assert gridCleint.connected();

                return new Consumer<ClusterState>() {
                    @Override public void accept(ClusterState state) {
                        try {
                            gridCleint.state().state(state, true);
                        }
                        catch (GridClientException e) {
                            throw new IgniteException(e.getMessage(), e);
                        }
                    }
                };
            }

            default:
                throw new IllegalArgumentException("Unsupported operation node type: " + operationNodeType);
        }
    }

    /**
     * Node type of change state operation.
     */
    public enum NodeType {
        /** Server node for Java API call the or the JMX call. */
        SERVER,

        /** Client node for Java API call the or the JMX call. */
        CLIENT,

        /**
         * Call from the thin client.
         *
         * @see IgniteClient
         */
        THIN_CLIENT,

        /**
         * Call from a remote control like control.sh of the REST API.
         *
         * @see GridClient
         * @see GridRestProcessor
         */
        GRID_CLIENT
    }
}
