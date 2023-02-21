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
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import javax.management.JMException;
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
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
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
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests permissions of cluster ctate change.
 */
@RunWith(Parameterized.class)
public class ClusterStatePermissionsTest extends AbstractSecurityTest {
    /** From-client flag. */
    @Parameterized.Parameter
    public NodeType nodeType;

    /** Persistence flag. */
    @Parameterized.Parameter(1)
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "nodeType={0}, persistence={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {NodeType.SERVER, false},
            {NodeType.SERVER, true},
            {NodeType.CLIENT, false},
            {NodeType.CLIENT, true},
            {NodeType.THIN_CLIENT, false},
            {NodeType.THIN_CLIENT, true},
            {NodeType.GRID_CLIENT, false},
            {NodeType.GRID_CLIENT, true},
            {NodeType.MX_BEAN, false},
            {NodeType.MX_BEAN, true}
        });
    }

    /** */
    private SecurityPermission[] testPerms = new SecurityPermission[0];

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        if (nodeType == NodeType.GRID_CLIENT)
            cfg.setConnectorConfiguration(new ConnectorConfiguration());

        SecurityPermissionSetBuilder secBuilder = create().defaultAllowAll(false);

        if (cfg.isClientMode()) {
            secBuilder.appendSystemPermissions(testPerms);

//            secBuilder.appendTaskPermissions(
//                "org.apache.ignite.internal.processors.cluster.ClientSetClusterStateComputeRequest",
//                TASK_EXECUTE
//            );
        }
        else {
            secBuilder.appendSystemPermissions(F.concat(testPerms, JOIN_AS_SERVER, CACHE_CREATE));

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));

            cfg.setCacheConfiguration(defaultCacheConfiguration());

            cfg.setClusterStateOnStart(INACTIVE);
        }

        TestSecurityPluginProvider secPlugin = new TestSecurityPluginProvider(
            instanceName,
            "",
            secBuilder.build(),
            false,
            new TestSecurityData(
                "client",
                "",
                create().defaultAllowAll(false).appendSystemPermissions(testPerms).build(),
                new Permissions()
            )
        );

        cfg.setPluginProviders(secPlugin);

        return cfg;
    }

    /** */
    @Test
    public void testActivationDeactivationAllowed() throws Exception {
        testPerms = F.asArray(ADMIN_OPS);

        doTestChangeState(
            F.asArray(ACTIVE, INACTIVE, ACTIVE_READ_ONLY, INACTIVE),
            F.asArray(TRUE, TRUE, TRUE, TRUE)
        );
    }

    /** */
    @Test
    public void testActivationNotAllowed() throws Exception {
        doTestChangeState(F.asArray(ACTIVE), F.asArray(FALSE));
    }

    /** */
    @Test
    public void testActivationAllowedDeactivationNotAllowed() throws Exception {
        testPerms = F.asArray(ADMIN_OPS);

        Ignite ig = startGrids(1);

        ig.cluster().state(ACTIVE);

        assertEquals(ACTIVE, ig.cluster().state());

        testPerms = new SecurityPermission[0];

        doTestChangeState(F.asArray(INACTIVE), F.asArray(FALSE));
    }

    /** */
    private void doTestChangeState(ClusterState[] states, Boolean[] allowed) throws Exception {
        assert states != null && allowed != null && states.length == allowed.length && states.length > 0;

        Ignite ig = startGrid(G.allGrids().size());

        startGrid(G.allGrids().size());
        startGrid(G.allGrids().size());

        Consumer<ClusterState> action = nodeStateAction(ig);

        for (int i = 0; i < states.length; ++i) {
            assert allowed[i] != null;

            ClusterState stateBefore = ig.cluster().state();
            ClusterState stateTo = states[i];

            assert stateBefore != stateTo;

            if (allowed[i]) {
                action.accept(stateTo);

                assertNotEquals(stateBefore, ig.cluster().state());
                assertEquals(stateTo, ig.cluster().state());
            }
            else {
                ensureThrows(action, stateTo);

                assertEquals(stateBefore, ig.cluster().state());
                assertNotEquals(stateTo, ig.cluster().state());
            }
        }
    }

    /** */
    private void ensureThrows(Consumer<ClusterState> action, ClusterState stateTo) {
        Class<? extends Throwable> cause;
        String errMsg;

        if (NodeType.THIN_CLIENT == nodeType) {
            cause = ClientAuthorizationException.class;
            errMsg = "User is not authorized to perform this operation";
        }
        else {
            cause = IgniteException.class;
            errMsg = "Authorization failed [perm=" + ADMIN_OPS;
        }

        assertThrows(
            null,
            () -> {
                action.accept(stateTo);

                return null;
            },
            cause,
            errMsg
        );
    }

    /** */
    private Consumer<ClusterState> nodeStateAction(Ignite srv) throws Exception {
        switch (nodeType) {
            case SERVER: {
                return new Consumer<ClusterState>() {
                    /** {@inheritDoc} */
                    @Override public void accept(ClusterState state) {
                        srv.cluster().state(state);
                    }
                };
            }

            case CLIENT: {
                Ignite client = startClientGrid(G.allGrids().size());

                return new Consumer<ClusterState>() {
                    /** {@inheritDoc} */
                    @Override public void accept(ClusterState state) {
                        client.cluster().state(state);
                    }
                };
            }

            case THIN_CLIENT: {
                IgniteClient thinClient = G.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                    .setUserName("client").setUserPassword(""));

                return new Consumer<ClusterState>() {
                    /** {@inheritDoc} */
                    @Override public void accept(ClusterState state) {
                        thinClient.cluster().state(state);
                    }
                };
            }

            case GRID_CLIENT: {
                GridClientConfiguration cfg = new GridClientConfiguration();

                cfg.setServers(F.asList("127.0.0.1:11211"));

                cfg.setSecurityCredentialsProvider(
                    new SecurityCredentialsBasicProvider(new SecurityCredentials("client", "")));

                GridClient gridCleint = GridClientFactory.start(cfg);

                assert gridCleint.connected();

                return new Consumer<ClusterState>() {
                    /** {@inheritDoc} */
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

            case MX_BEAN: {
                IgniteMXBean igniteMXBean = getMxBean(srv.name(), "Kernal", "IgniteKernal", IgniteMXBean.class);

                return new Consumer<ClusterState>() {
                    /** {@inheritDoc} */
                    @Override public void accept(ClusterState state) {
                        try {
                            igniteMXBean.clusterState(state.name(), true);
                        }
                        catch (JMException e) {
                            throw new IgniteException(e.getMessage(), e);
                        }
                    }
                };
            }

            default:
                throw new IllegalArgumentException("Unsupported node type: " + nodeType);
        }
    }

    /** State change request node type. */
    public enum NodeType {
        /** */
        SERVER,

        /** */
        CLIENT,

        /** */
        THIN_CLIENT,

        /** */
        GRID_CLIENT,

        /** */
        MX_BEAN
    }
}
