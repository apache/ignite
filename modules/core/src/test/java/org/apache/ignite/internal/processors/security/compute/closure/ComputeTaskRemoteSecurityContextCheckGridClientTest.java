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

package org.apache.ignite.internal.processors.security.compute.closure;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;

/**
 * Testing operation security context when the compute task launched from GridClient is executed on remote nodes.
 * <p>
 * The GridClient broadcasts a task to 'run' nodes that starts compute task. That compute task is executed on
 * 'check' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
public class ComputeTaskRemoteSecurityContextCheckGridClientTest extends AbstractRemoteSecurityContextCheckTest {
    /** */
    private static final String CLIENT_LOGIN = "userLogin";

    /** */
    private static final String CLIENT_PASSWORD = "userPwd";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void test() throws Exception {
        // Setup VERIFIER when task started
        grid(SRV_RUN)
            .events()
            .localListen(localListener(), EVT_TASK_STARTED);

        try (GridClient client = GridClientFactory.start(gridClientConfiguration())) {
            client.compute().execute(ComputeTaskClosure.class.getName(), new T2<>(nodesToCheckIds(), endpointIds()));
        }

        VERIFIER.checkResult();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setConnectorConfiguration(new ConnectorConfiguration().setHost("127.0.0.1"))
            .setIncludeEventTypes(EVT_TASK_STARTED);
    }

    /** */
    private GridClientConfiguration gridClientConfiguration() {
        SecurityCredentials creds = new SecurityCredentials(CLIENT_LOGIN, CLIENT_PASSWORD);

        SecurityCredentialsBasicProvider credProv = new SecurityCredentialsBasicProvider(creds);

        return new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(credProv)
            .setBalancer(nodes ->
                nodes.stream().filter(n -> n.nodeId().equals(grid(SRV_RUN).localNode().id())).findFirst().get());
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToRun() {
        return Collections.singletonList(SRV_RUN);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Collections.singletonList(SRV_CHECK);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> endpoints() {
        return Collections.singletonList(SRV_ENDPOINT);
    }

    /** */
    private IgnitePredicate<Event> localListener() {
        return new IgnitePredicate<Event>() {
            /** */
            @Override public boolean apply(Event evt) {
                SecuritySubject subj = IgnitionEx.localIgnite().context().security().securityContext().subject();

                VERIFIER.initiator(subj.id());

                setupVerifier(VERIFIER);

                return false;
            }
        };
    }

    /**
     * Compute task for tests.
     */
    static class ComputeTaskClosure implements ComputeTask<T2<Collection<UUID>, Collection<UUID>>, Integer> {
        /** Local ignite. */
        @IgniteInstanceResource
        protected transient IgniteEx loc;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable T2<Collection<UUID>, Collection<UUID>> arg) {
            VERIFIER.register(OPERATION_START);

            /** Collection of transition node ids. */
            Collection<UUID> remotes = arg.get1();

            /** Collection of endpoint node ids. */
            Collection<UUID> endpoints = arg.get2();

            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            for (UUID id : remotes) {
                res.put(
                    new ComputeJob() {
                        @IgniteInstanceResource
                        private IgniteEx loc;

                        @Override public void cancel() {
                            // no-op
                        }

                        @Override public Object execute() {
                            Object login = loc.context().security().securityContext().subject().login();

                            assertEquals(CLIENT_LOGIN, login);

                            VERIFIER.register(OPERATION_CHECK);

                            compute(loc, endpoints).broadcast(() -> VERIFIER.register(OPERATION_ENDPOINT));

                            return null;
                        }
                    },
                    loc.cluster().node(id));
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            Object login = loc.context().security().securityContext().subject().login();

            assertEquals(CLIENT_LOGIN, login);

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            Object login = loc.context().security().securityContext().subject().login();

            assertEquals(CLIENT_LOGIN, login);

            return null;
        }
    }
}
