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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXE;

/** */
public class RestRequestProcessingMessagesTest extends GridCommonAbstractTest {
    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private static final int BINARY_PORT = 11211;

    /** */
    private static final LogListener REQ_RECEIVED_LSNR = LogListener.matches(
        Pattern.compile("REST request received \\[req=.+]\\.")).build();

    /** */
    private static final LogListener REQ_SUCCEED_LSNR = LogListener.matches(
        Pattern.compile("REST request result \\[req=.+, resp=.+]\\.")).build();

    /** */
    private static final LogListener REQ_FAILED_LSNR = LogListener.matches(
        Pattern.compile("REST request failed \\[req=.+, err=(.+\\s?)*]\\.")).build();

    /** */
    private static final LogListener FUT_CANCELLED_LSNR = LogListener.matches(
        Pattern.compile("REST request failed \\[req=.+, err=Future was cancelled \\[fut=.+]]\\.")).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger log = new ListeningTestLogger(true, log());

        log.registerListener(REQ_FAILED_LSNR);
        log.registerListener(REQ_SUCCEED_LSNR);
        log.registerListener(REQ_RECEIVED_LSNR);
        log.registerListener(FUT_CANCELLED_LSNR);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setConnectorConfiguration(
                new ConnectorConfiguration()
                    .setHost(HOST)
                    .setPort(BINARY_PORT)
            );
    }

    /** */
    @Test
    public void testRestRequestMessages() throws Exception {
        IgniteEx ignite = startGrid(1);

        GridRestProtocolHandler hnd = U.field(ignite.context().rest(), "protoHnd");

        GridRestChangeStateRequest validReq = new GridRestChangeStateRequest();

        validReq.active(true);
        validReq.command(CLUSTER_ACTIVATE);

        checkListeners(() -> hnd.handleAsync(validReq), REQ_RECEIVED_LSNR, REQ_SUCCEED_LSNR);

        checkListeners(() -> hnd.handleAsync(new GridRestChangeStateRequest()), REQ_RECEIVED_LSNR, REQ_FAILED_LSNR);

        GridRestTaskRequest cancelledReq = new GridRestTaskRequest();

        cancelledReq.command(EXE);
        cancelledReq.taskName(StuckTask.class.getName());

        checkListeners(() -> {
            try {
                hnd.handleAsync(cancelledReq).cancel();
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }, REQ_RECEIVED_LSNR, REQ_FAILED_LSNR);
    }

    /** */
    private void checkListeners(Runnable r, LogListener... lsnr) throws Exception{
        Arrays.stream(lsnr).forEach(LogListener::reset);

        r.run();

        assertTrue(GridTestUtils.waitForCondition(() ->
            Arrays.stream(lsnr).allMatch(LogListener::check), getTestTimeout()));
    }

    /** */
    private static class StuckTask extends ComputeTaskAdapter<String, String> {
        /** */
        public final CountDownLatch latch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) throws IgniteException {
            try {
               latch.await();
            }
            catch (InterruptedException ignored) {
            }

            return F.asMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // No-op
                    }

                    @Override public Object execute() throws IgniteException {
                        return null;
                    }
                },
                subgrid.get(0)
            );
        }

        /** {@inheritDoc} */
        @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
