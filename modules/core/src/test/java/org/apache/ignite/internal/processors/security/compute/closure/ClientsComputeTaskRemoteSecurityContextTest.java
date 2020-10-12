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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonList;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Testing operation security context when the compute task is executed on remote nodes.
 */
@RunWith(Parameterized.class)
public class ClientsComputeTaskRemoteSecurityContextTest extends AbstractRemoteSecurityContextCheckTest {
    /** */
    @Parameterized.Parameters(name = "async={0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(new Boolean[] {false}, new Boolean[] {true});
    }

    /** */
    @Parameterized.Parameter()
    public boolean async;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_RUN);

        startGridAllowAll(SRV_CHECK);

        startClientAllowAll(CLNT_CHECK).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testGridClient() throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration()
            .setServers(singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials("grid", "")))
            .setBalancer(nodes ->
                nodes.stream().findFirst().orElseThrow(NoSuchElementException::new));

        try (GridClient clnt = GridClientFactory.start(cfg)) {
            VERIFIER.initiator(clnt.id());

            setupVerifier(VERIFIER);

            GridClientCompute comp = clnt.compute().projection(clnt.compute().nodes(nodesToRunIds()));

            if (async)
                comp.executeAsync(ComputeTaskClosure.class.getName(), null).get();
            else
                comp.execute(ComputeTaskClosure.class.getName(), null);

            VERIFIER.checkResult();
        }
    }

    /** */
    @Test
    public void testIgniteClient() throws Exception {
        String login = async ? "thin_async" : "thin";

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(login)
            .setUserPassword("");

        try (IgniteClient clnt = Ignition.startClient(cfg)) {
            UUID clntId = grid(SRV_RUN).context().security().authenticatedSubjects()
                .stream().filter(s -> F.eq(s.login(), login)).findFirst()
                .orElseThrow(NoSuchElementException::new).id();

            VERIFIER.initiator(clntId);

            setupVerifier(VERIFIER);

            ClientCompute comp = clnt.compute(clnt.cluster().forNodeIds(nodesToRunIds()));

            if (async)
                comp.executeAsync(ComputeTaskClosure.class.getName(), null).get();
            else
                comp.execute(ComputeTaskClosure.class.getName(), null);

            VERIFIER.checkResult();
        }
    }

    /** */
    @Test
    public void testRestClient() throws Exception {
        UUID clntId = UUID.randomUUID();

        GridRestTaskRequest req = new GridRestTaskRequest();

        req.credentials(new SecurityCredentials("rest", ""));
        req.clientId(clntId);
        req.command(GridRestCommand.EXE);
        req.async(async);
        req.taskName(ComputeTaskClosure.class.getName());

        VERIFIER.initiator(clntId);

        setupVerifier(VERIFIER);

        GridRestResponse res = restProtocolHandler(grid(SRV_RUN)).handle(req);

        if (async) {
            waitForCondition(() -> {
                GridRestTaskRequest resReq = new GridRestTaskRequest();

                resReq.credentials(new SecurityCredentials("rest", ""));
                resReq.command(GridRestCommand.RESULT);
                resReq.taskId(((GridClientTaskResultBean)res.getResponse()).getId());

                GridRestResponse rslt;

                try {
                    rslt = restProtocolHandler(grid(SRV_RUN)).handle(resReq);
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }

                return ((GridClientTaskResultBean)rslt.getResponse()).isFinished();
            }, 10_000);
        }

        VERIFIER.checkResult();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName,
        AbstractTestSecurityPluginProvider pluginProv) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1)));
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToRun() {
        return Collections.singleton(SRV_RUN);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> endpoints() {
        return Collections.emptyList();
    }

    /**
     * Compute task for tests.
     */
    static class ComputeTaskClosure implements ComputeTask<Object, Integer> {
        /** Local ignite. */
        @IgniteInstanceResource
        protected transient IgniteEx loc;

        /** Default constructor. */
        public ComputeTaskClosure() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
            VERIFIER.register(OPERATION_START);

            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            loc.cluster().nodes().stream().filter(n -> {
                String login = SecurityUtils.nodeSecurityContext(loc.context().marshallerContext().jdkMarshaller(),
                    U.resolveClassLoader(loc.context().config()), n)
                    .subject().login().toString();

                return F.eq(SRV_CHECK, login) || F.eq(CLNT_CHECK, login);
            }).forEach(n -> res.put(
                new ComputeJob() {
                    @Override public void cancel() {
                        // no-op
                    }

                    @Override public Object execute() {
                        VERIFIER.register(OPERATION_CHECK);

                        return null;
                    }
                }, n));

            return res;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
