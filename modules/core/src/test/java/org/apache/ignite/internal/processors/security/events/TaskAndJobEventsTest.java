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

package org.apache.ignite.internal.processors.security.events;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientTaskResultBean;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonList;
import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_JOB_QUEUED;
import static org.apache.ignite.events.EventType.EVT_JOB_RESULTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.events.EventType.EVT_TASK_REDUCED;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests that an event's local listener and an event's remote filter get correct subjectId when task's or job's
 * operations are performed.
 */
@RunWith(Parameterized.class)
public class TaskAndJobEventsTest extends AbstractSecurityTest {
    /** Types of events. */
    private static final int[] EVENT_TYPES = new int[] {
        EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_TASK_REDUCED,
        EVT_JOB_MAPPED, EVT_JOB_RESULTED, EVT_JOB_STARTED, EVT_JOB_FINISHED, EVT_JOB_QUEUED};

    /** Types of events. */
    private static final int[] TIMEDOUT_EVENT_TYPES = new int[] {
        EVT_TASK_STARTED, EVT_TASK_TIMEDOUT, EVT_TASK_FAILED, EVT_JOB_MAPPED,
        EVT_JOB_QUEUED, EVT_JOB_STARTED, EVT_JOB_CANCELLED, EVT_JOB_FAILED
    };

    /** Job's sleep time. */
    private static final long TIME_TO_SLEEP = 2000L;

    /** Timeout. */
    private static final long TIMEOUT = 1000L;

    /** Remote events. */
    private static final Set<Integer> rmtSet = Collections.synchronizedSet(new HashSet<>());

    /** Local events. */
    private static final Set<Integer> locSet = Collections.synchronizedSet(new HashSet<>());

    /** Test task name. */
    private static final String TASK_NAME = "org.apache.ignite.internal.processors.security.events.TaskAndJobEventsTest$TestComputeTask";

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    private static final String CLNT = "client";

    /** Server node. */
    private static final String SRV = "server";

    /** Expected login. */
    @Parameterized.Parameter
    public String expLogin;

    /** Async mode. */
    @Parameterized.Parameter(1)
    public Boolean async;

    /** Task should be timed out. */
    @Parameterized.Parameter(2)
    public boolean timedout;

    /** Parameters. */
    @Parameterized.Parameters(name = "expLogin={0}, async={1}, timedout={2}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        Stream.of(SRV, CLNT, "thin", "rest", "grid").forEach(login -> {
            res.add(new Object[] {login, false, false});
            res.add(new Object[] {login, true, false});

            if (!"grid".equals(login)) {
                res.add(new Object[] {login, false, true});
                res.add(new Object[] {login, true, true});
            }
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startClientAllowAll(CLNT);
    }

    /** */
    @Test
    public void test() throws Exception {
        int[] evtTypes = timedout ? TIMEDOUT_EVENT_TYPES : EVENT_TYPES;

        Arrays.stream(evtTypes).forEach(type -> {
            locSet.add(type);
            rmtSet.add(type);
        });

        UUID taskLsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    locSet.remove(evt.type());

                    onEvent(ign, evt, expLogin);

                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    rmtSet.remove(evt.type());

                    onEvent(ign, evt, expLogin);

                    return true;
                }
            }, evtTypes);

        try {
            if (timedout) {
                assertThrowsWithCause(operation(),
                    "thin".equals(expLogin) ? ClientServerError.class : ComputeTaskTimeoutException.class);
            }
            else
                operation().run();

            waitForCondition(locSet::isEmpty, 10_000);
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(taskLsnrId);
        }

        assertTrue("Remote filter. Events that are not happen: " + rmtSet.stream().map(U::gridEventName),
            rmtSet.isEmpty());
        assertTrue("Local listener. Events that are not happen: " + locSet.stream().map(U::gridEventName),
            locSet.isEmpty());
    }

    /**
     * @return Operation to test.
     */
    private GridTestUtils.RunnableX operation() {
        final Long timeToSleep = timedout ? TIME_TO_SLEEP : null;

        if (SRV.equals(expLogin) || CLNT.equals(expLogin)) {
            return () -> {
                IgniteCompute cmp = grid(expLogin).compute();

                if (timedout)
                    cmp = cmp.withTimeout(TIMEOUT);

                if (async)
                    cmp.executeAsync(TASK_NAME, timeToSleep).get();
                else
                    cmp.execute(TASK_NAME, timeToSleep);
            };
        }
        else if ("thin".equals(expLogin)) {
            return () -> {
                try (IgniteClient clnt = startClient()) {
                    ClientCompute cmp = clnt.compute();

                    if (timedout)
                        cmp = cmp.withTimeout(TIMEOUT);

                    if (async)
                        cmp.executeAsync(TASK_NAME, timeToSleep).get();
                    else
                        cmp.execute(TASK_NAME, timeToSleep);
                }
            };
        }
        else if ("grid".equals(expLogin)) {
            return () -> {
                try (GridClient client = startGridClient()) {
                    GridClientCompute cmp = client.compute();

                    if (async)
                        cmp.executeAsync(TASK_NAME, null).get();
                    else
                        cmp.execute(TASK_NAME, null);
                }
            };
        }
        else if ("rest".equals(expLogin)) {
            return () -> {
                GridRestTaskRequest req = new GridRestTaskRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.command(GridRestCommand.EXE);
                req.taskName(TASK_NAME);
                req.async(async);
                req.timeout(timedout ? TIMEOUT : 0L);
                req.params(singletonList(timeToSleep));

                GridRestResponse res = restProtocolHandler(grid(LISTENER_NODE)).handle(req);

                if (async && timedout) {
                    GridRestTaskRequest resReq = new GridRestTaskRequest();

                    resReq.credentials(new SecurityCredentials("rest", ""));
                    resReq.command(GridRestCommand.RESULT);
                    resReq.taskId(((GridClientTaskResultBean)res.getResponse()).getId());

                    TimeUnit.MILLISECONDS.sleep(timeToSleep);

                    res = restProtocolHandler(grid(LISTENER_NODE)).handle(resReq);
                }

                if (res.getError() != null && res.getError().contains("Task timed out"))
                    throw new ComputeTaskTimeoutException("Task timed out");
            };
        }

        throw new IllegalArgumentException("Unknown login " + expLogin);
    }

    /** */
    private static void onEvent(IgniteEx ign, Event evt, String expLogin) {
        assert evt instanceof TaskEvent || evt instanceof JobEvent;

        UUID actualSubjId = evt instanceof TaskEvent ? ((TaskEvent)evt).subjectId() : ((JobEvent)evt).taskSubjectId();

        try {
            SecuritySubject subj = ign.context().security().authenticatedSubject(actualSubjId);

            assertEquals(expLogin, subj.login().toString());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Test compute task. */
    public static class TestComputeTask implements ComputeTask<Long, String> {
        /** Default constructor. */
        public TestComputeTask() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            assert !subgrid.isEmpty();

            return F.asMap(new ComputeJob() {
                @Override public void cancel() {
                    // No-op.
                }

                @Override public Object execute() throws IgniteException {
                    try {
                        if (arg != null)
                            TimeUnit.MILLISECONDS.sleep(arg);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException(e);
                    }

                    return null;
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable String reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1)))
            .setIncludeEventTypes(EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_TASK_FAILED, EVT_TASK_TIMEDOUT,
                EVT_TASK_REDUCED, EVT_JOB_MAPPED, EVT_JOB_RESULTED, EVT_JOB_STARTED, EVT_JOB_FINISHED, EVT_JOB_FAILED,
                EVT_JOB_QUEUED, EVT_JOB_CANCELLED);
    }

    /** */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setUserName(expLogin)
                .setUserPassword("")
        );
    }

    /** */
    private GridClient startGridClient() throws GridClientException {
        return GridClientFactory.start(
            new GridClientConfiguration()
                .setServers(singletonList("127.0.0.1:11211"))
                .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials("grid", "")))
                .setBalancer(nodes ->
                    nodes.stream().findFirst().orElseThrow(NoSuchElementException::new))
        );
    }
}
