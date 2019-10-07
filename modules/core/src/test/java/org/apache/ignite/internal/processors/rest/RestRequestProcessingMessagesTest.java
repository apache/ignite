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

package org.apache.ignite.internal.processors.rest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.request.GridRestChangeStateRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;

/**
 * Tests messages occured while rest request processing.
 */
public class RestRequestProcessingMessagesTest extends GridCommonAbstractTest {
    /** Listener of received request message. */
    private static final LogListener REQ_RCV_LSNR = LogListener.matches(
        Pattern.compile("REST request received \\[req=.+]")).build();

    /** Listener of succeed request message. */
    private static final LogListener REQ_SUCCEED_LSNR = LogListener.matches(
        Pattern.compile("REST request result \\[req=.+, resp=.+]")).build();

    /** Listener of failed request message. */
    private static final LogListener REQ_FAILED_LSNR = LogListener.matches(
        Pattern.compile("REST request failed \\[req=.+, err=(.+\\s?)+]")).build();

    /** Listener of cancelled future message. */
    private static final LogListener FUT_CANCELLED_LSNR = LogListener.matches(
        Pattern.compile("REST request future was cancelled \\[req=.+, fut=.+]")).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger log = new ListeningTestLogger(true, log());

        log.registerListener(REQ_FAILED_LSNR);
        log.registerListener(REQ_SUCCEED_LSNR);
        log.registerListener(REQ_RCV_LSNR);
        log.registerListener(FUT_CANCELLED_LSNR);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setConnectorConfiguration(
                new ConnectorConfiguration()
            );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestRequestMessages() throws Exception {
        IgniteEx ignite = startGrid(1);

        GridRestProtocolHandler hnd = U.field(ignite.context().rest(), "protoHnd");

        GridRestChangeStateRequest validReq = new GridRestChangeStateRequest();

        validReq.active(true);
        validReq.command(CLUSTER_ACTIVATE);

        checkListeners(() -> hnd.handleAsync(validReq), REQ_RCV_LSNR, REQ_SUCCEED_LSNR);

        checkListeners(() -> hnd.handleAsync(new GridRestChangeStateRequest()), REQ_RCV_LSNR, REQ_FAILED_LSNR);

        GridRestTaskRequest cancelledReq = new GridRestTaskRequest();

        cancelledReq.command(EXE);
        cancelledReq.taskName(StuckTask.class.getName());
        cancelledReq.params(Collections.singletonList(getTestTimeout()));

        checkListeners(() -> hnd.handleAsync(cancelledReq).cancel(), REQ_RCV_LSNR, FUT_CANCELLED_LSNR);
    }

    /**
     * Verifies that specified listeners intercepted the message produced by {@code r} execution.
     *
     * @param r {@link Runnable} after which execution messeges will be checked.
     * @param lsnrs Listeners to be checked.
     */
    private void checkListeners(RunnableX r, LogListener... lsnrs) throws Exception {
        Arrays.stream(lsnrs).forEach(LogListener::reset);

        r.run();

        assertTrue(GridTestUtils.waitForCondition(() ->
            Arrays.stream(lsnrs).allMatch(LogListener::check), getTestTimeout()));
    }

    /**
     * Test task, which provides ability to imitate task processing stuck.
     * First task parameter is used as waiting time upper bound.
     */
    private static class StuckTask extends ComputeTaskAdapter<Long, Void> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            try {
                new CountDownLatch(1).await(arg, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
            }

            return F.asMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() throws IgniteException {
                        return null;
                    }
                },
                subgrid.get(0)
            );
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
