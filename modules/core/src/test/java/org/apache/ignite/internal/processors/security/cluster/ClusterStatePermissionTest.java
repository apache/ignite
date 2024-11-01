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
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientException;
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
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

    /** The initiator parameter. */
    @Parameterized.Parameter
    public Initiator initiator;

    /** Persistence flag parameter. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "initiator={0}, persistence={1}")
    public static Collection<?> parameters() {
        return cartesianProduct(asList(Initiator.values()), asList(false, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        SecurityPermission[] srvPerms = initiator == Initiator.SERVER || initiator == Initiator.CLIENT ? permissions :
            EMPTY_PERMS;

        // Apply the permissions on client side if they are not for server side.
        SecurityPermission[] clientPerms = F.isEmpty(srvPerms) ? permissions : EMPTY_PERMS;

        return getConfiguration(instanceName, srvPerms, clientPerms);
    }

    /**
     * @return Ignite configuration with given server and client permissions.
     */
    private IgniteConfiguration getConfiguration(
        String instanceName,
        SecurityPermission[] srvPerms,
        SecurityPermission[] cliPerms
    ) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        TestSecurityPluginProvider secPlugin = new TestSecurityPluginProvider(
            instanceName,
            "",
            create().defaultAllowAll(false).appendSystemPermissions(F.concat(srvPerms, JOIN_AS_SERVER)).build(),
            false,
            new TestSecurityData(
                "client",
                "",
                create().defaultAllowAll(false).appendSystemPermissions(cliPerms).build(),
                new Permissions()
            )
        );

        cfg.setPluginProviders(secPlugin);

        cfg.setClusterStateOnStart(INACTIVE);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));
        }

        return cfg;
    }

    /**
     * Tests that all state changes are allowed with the permission.
     */
    @Test
    public void testActivationDeactivationAllowed() throws Exception {
        permissions = F.asArray(ADMIN_CLUSTER_STATE);

        doTestChangeState(
            F.asArray(ACTIVE_READ_ONLY, INACTIVE, ACTIVE_READ_ONLY, ACTIVE, INACTIVE, ACTIVE, ACTIVE_READ_ONLY),
            true
        );
    }

    /**
     * Tests that the activation is not allowed without the permission.
     */
    @Test
    public void testActivationNotAllowed() throws Exception {
        doTestChangeState(F.asArray(ACTIVE), false);
    }

    /**
     * Tests that the deactivation is not allowed without the permission.
     */
    @Test
    public void testDeactivationNotAllowed() throws Exception {
        startAllAllowedNode();

        doTestChangeState(F.asArray(INACTIVE), false);
    }

    /**
     * Tests that same-state change is not allowed without the permission.
     */
    @Test
    public void testSameStateNotAllowedWithoutPermission() throws Exception {
        startAllAllowedNode();

        doTestChangeState(F.asArray(ACTIVE), false);
    }

    /**
     * Starts single server node, activates the cluster.
     */
    private void startAllAllowedNode() throws Exception {
        Ignite ig = startGrid(getConfiguration(getTestIgniteInstanceName(0), F.asArray(ADMIN_CLUSTER_STATE),
            EMPTY_PERMS));

        ig.cluster().state(ACTIVE);
    }

    /**
     * Tries to change state and ensures that a proper error raises if required.
     *
     * @param states Cluster states to try to set to one by one.
     * @param allowed {@code True}, if the state changes must be allowed. {@code False} otherwise.
     */
    private void doTestChangeState(ClusterState[] states, boolean allowed) throws Exception {
        assert states != null && states.length > 0;

        Ignite ig = startGrid(G.allGrids().size());

        startGrid(G.allGrids().size());

        Consumer<ClusterState> action = initiatorAction(ig);

        for (int i = 0; i < states.length; ++i) {
            ClusterState stateTo = states[i];

            if (allowed) {
                action.accept(stateTo);

                assertEquals(stateTo, ig.cluster().state());
            }
            else
                ensureThrows(action, stateTo);
        }
    }

    /**
     * Ensures that a proper error occurs on cluster change state action.
     */
    private void ensureThrows(Consumer<ClusterState> action, ClusterState stateTo) {
        String errMsg = "Authorization failed [perm=" + ADMIN_CLUSTER_STATE;
        Class<? extends Throwable> cause;

        if (Initiator.THIN_CLIENT == initiator)
            cause = ClientException.class;
        else if (Initiator.REMOTE_CONTROL == initiator)
            cause = GridClientException.class;
        else
            cause = SecurityException.class;

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
     * @return Change state operation depending on {@link #initiator}.
     */
    private Consumer<ClusterState> initiatorAction(Ignite srv) throws Exception {
        switch (initiator) {
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
                return new Consumer<ClusterState>() {
                    @Override public void accept(ClusterState state) {
                        try (IgniteClient client = G.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                            .setUserName("client").setUserPassword(""))) {
                            client.cluster().state(state);
                        }
                    }
                };
            }

            case REMOTE_CONTROL: {
                GridClientConfiguration cfg = new GridClientConfiguration();

                cfg.setServers(asList("127.0.0.1:11211"));

                cfg.setSecurityCredentialsProvider(
                    new SecurityCredentialsBasicProvider(new SecurityCredentials("client", "")));

                return new Consumer<ClusterState>() {
                    @Override public void accept(ClusterState state) {
                        try (GridClient gridClient = GridClientFactory.start(cfg)) {
                            assert gridClient.connected();

                            gridClient.state().state(state, true);
                        }
                        catch (GridClientException e) {
                            throw new IgniteException(e.getMessage(), e);
                        }
                    }
                };
            }

            default:
                throw new IllegalArgumentException("Unsupported operation initiator: " + initiator);
        }
    }

    /**
     * Initiator type of the cluster state change.
     */
    private enum Initiator {
        /**
         * Server node for Java API call the or the JMX call.
         *
         * @see IgniteCluster#state(ClusterState)
         */
        SERVER,

        /**
         * Client node for Java API call the or the JMX call.
         *
         * @see IgniteCluster#state(ClusterState)
         * @see IgniteConfiguration#isClientMode()
         */
        CLIENT,

        /**
         * Call from the thin client.
         *
         * @see ClientCluster#state(ClusterState)
         */
        THIN_CLIENT,

        /**
         * Call from a remote control like control.sh of the REST API.
         *
         * @see GridClient
         * @see GridRestCommand#CLUSTER_SET_STATE
         */
        REMOTE_CONTROL
    }
}
