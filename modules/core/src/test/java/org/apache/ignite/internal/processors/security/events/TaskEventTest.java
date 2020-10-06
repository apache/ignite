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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_JOB_QUEUED;
import static org.apache.ignite.events.EventType.EVT_JOB_RESULTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.events.EventType.EVT_TASK_REDUCED;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;

@RunWith(Parameterized.class)
public class TaskEventTest extends AbstractSecurityTest {
    /** Array types of events. */
    private static final int[] EVENT_TYPES = new int[] {
        EVT_TASK_STARTED, EVT_TASK_FINISHED, EVT_TASK_REDUCED,
        EVT_JOB_MAPPED, EVT_JOB_RESULTED, EVT_JOB_STARTED, EVT_JOB_FINISHED, EVT_JOB_QUEUED};

    /** Remote counter. */
    private static final AtomicInteger rmtCnt = new AtomicInteger();

    /** Local counter. */
    private static final AtomicInteger locCnt = new AtomicInteger();

    /** Test task name. */
    private static final String TASK_NAME = "org.apache.ignite.internal.processors.security.events.TaskEventTest$TestComputeTask";

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    private static final String CLNT = "client";

    /** Server node. */
    private static final String SRV = "server";

    /** Events latch. */
    private static CountDownLatch evtsLatch;

    /** Expected login. */
    @Parameterized.Parameter
    public String expLogin;

    /** Async mode. */
    @Parameterized.Parameter(1)
    public Boolean async;

    /** Parameters. */
    @Parameterized.Parameters(name = "expLogin={0}, async={1}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (String l : Arrays.asList(SRV, CLNT, "thin", "rest")) {
            res.add(new Object[] {l, false});
            res.add(new Object[] {l, true});
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startClientAllowAll(CLNT);
    }

    @Test
    public void test() throws Exception {
        int expTimes = EVENT_TYPES.length;

        evtsLatch = new CountDownLatch(2 * expTimes);

        locCnt.set(0);
        rmtCnt.set(0);

        UUID taskLsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    onEvent(ign, locCnt, evt, expLogin);

                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    onEvent(ign, rmtCnt, evt, expLogin);

                    return true;
                }
            }, EVENT_TYPES);

        try {
            operation().run();

            evtsLatch.await(10, TimeUnit.SECONDS);
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(taskLsnrId);
        }

        assertEquals(expTimes, rmtCnt.get());
        assertEquals(expTimes, locCnt.get());
    }

    private GridTestUtils.RunnableX operation() {
        if (SRV.equals(expLogin) || CLNT.equals(expLogin)) {
            return () -> {
                IgniteCompute cmp = grid(expLogin).compute();

                if (async)
                    cmp.executeAsync(TASK_NAME, "");
                else
                    cmp.execute(TASK_NAME, "");
            };
        }
        else if ("thin".equals(expLogin)) {
            return () -> {
                ClientCompute cmp = startClient("thin").compute();

                if (async)
                    cmp.executeAsync(TASK_NAME, "").get();
                else
                    cmp.execute(TASK_NAME, "");
            };
        }
        else if ("rest".equals(expLogin)) {
            return () -> {
                GridRestTaskRequest req = new GridRestTaskRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.command(GridRestCommand.EXE);
                req.taskName(TASK_NAME);
                req.async(async);

                restProtocolHandler().handle(req);
            };
        }

        throw new IllegalArgumentException("Uncknown login " + expLogin);
    }

    /**
     *
     */
    private static void onEvent(IgniteEx ign, AtomicInteger cntr, Event evt, String expLogin) {
        assert evt instanceof TaskEvent || evt instanceof JobEvent;

        UUID actualSubjId = evt instanceof TaskEvent ? ((TaskEvent)evt).subjectId() : ((JobEvent)evt).taskSubjectId();

        cntr.incrementAndGet();

        evtsLatch.countDown();

        try {
            SecuritySubject subj = ign.context().security().authenticatedSubject(actualSubjId);

            assertEquals(expLogin, subj.login().toString());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Test compute task. */
    public static class TestComputeTask implements ComputeTask<String, String> {
        /** Default constructor. */
        public TestComputeTask() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) throws IgniteException {
            assert !subgrid.isEmpty();

            return F.asMap(new ComputeJob() {
                @Override public void cancel() {
                    // No-op.
                }

                @Override public Object execute() throws IgniteException {
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
            .setIncludeEventTypes(EVENT_TYPES);
    }

    /**
     *
     */
    private IgniteClient startClient(String expLogin) {
        return Ignition.startClient(
            new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setUserName(expLogin)
                .setUserPassword("")
        );
    }

    /**
     *
     */
    private GridRestProtocolHandler restProtocolHandler() throws Exception {
        Object restPrc = grid(LISTENER_NODE).context().components().stream()
            .filter(c -> c instanceof GridRestProcessor).findFirst()
            .orElseThrow(RuntimeException::new);

        Field fld = GridRestProcessor.class.getDeclaredField("protoHnd");

        fld.setAccessible(true);

        return (GridRestProtocolHandler)fld.get(restPrc);
    }
}
