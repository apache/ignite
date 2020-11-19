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

package org.apache.ignite.internal.client.rest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.probe.GridProbeCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test whether REST probe command works correctly when kernal has started and vice versa.
 */
public class GridProbeCommandTest extends GridCommonAbstractTest {
    /** */
    private static final int JETTY_PORT = 8080;

    /** */
    private CountDownLatch triggerRestCmdLatch = new CountDownLatch(1);

    /** */
    private CountDownLatch triggerPluginStartLatch = new CountDownLatch(1);

    /** */
    public static Map<String, Object> executeProbeRestRequest() throws IOException {
        HttpURLConnection conn = (HttpURLConnection)(new URL("http://localhost:" + JETTY_PORT + "/ignite?cmd=probe").openConnection());
        conn.connect();

        boolean isHTTP_OK = conn.getResponseCode() == HttpURLConnection.HTTP_OK;

        Map<String, Object> restResponse = null;

        try (InputStreamReader streamReader = new InputStreamReader(isHTTP_OK ? conn.getInputStream() : conn.getErrorStream())) {

            ObjectMapper objMapper = new ObjectMapper();
            restResponse = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info("probe command response is: " + restResponse);

        }
        catch (Exception e) {
            log.error("error executing probe rest command", e);
        }
        return restResponse;

    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        if (igniteInstanceName.equals("regular"))
            return cfg;
        else if (igniteInstanceName.equals("delayedStart")) {
            PluginProvider delayedStartPluginProvider = new DelayedStartPluginProvider(triggerPluginStartLatch, triggerRestCmdLatch);

            cfg.setPluginProviders(new PluginProvider[] {delayedStartPluginProvider});
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);
    }

    /**
     * Test for the REST probe command
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommand() throws Exception {
        startGrid("regular");

        GridRestCommandHandler hnd = new GridProbeCommandHandler((grid("regular")).context());

        GridRestCacheRequest req = new GridRestCacheRequest();
        req.command(GridRestCommand.PROBE);

        IgniteInternalFuture<GridRestResponse> resp = hnd.handleAsync(req);
        resp.get();

        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertEquals("grid has started", resp.result().getResponse());

    }

    /**
     * <p>Test rest cmd=probe command given a non fully started kernal. </p>
     * <p>1. start the grid on a seperate thread w/a plugin that will keep it waiting, at a point after rest http
     * processor is ready, until signaled to proceed. </p>
     * <p>2. when the grid.start() has reached the plugin init method(rest http processor has started now), issue a
     * rest command against the non-fully started kernal. </p>
     * <p>3. validate that the probe cmd has returned the appropriate erroneous code and message. </p>
     * <p>4. stop the grid. </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandGridNotStarted() throws Exception {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    startGrid("delayedStart");
                }
                catch (Exception e) {
                    log.error("error when starting delatedStart grid", e);
                }
            }
        }).start();

        Map<String, Object> probeRestCommandResponse;

        log.info("awaiting plugin handler latch");
        triggerPluginStartLatch.await();
        log.info("starting rest command url call");
        try {
            probeRestCommandResponse = executeProbeRestRequest();
            log.info("finished rest command url call");
        }
        finally {
            triggerRestCmdLatch.countDown(); //make sure the grid shuts down
        }

        assertTrue(probeRestCommandResponse.get("error").equals("grid has not started"));
        assertEquals(GridRestResponse.SERVICE_UNAVAILABLE, probeRestCommandResponse.get("successStatus"));
    }

    /**
     * <p>Start a regular grid, issue a cmd=probe rest command, and validate restponse
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommandGridStarted() throws Exception {
        startGrid("regular");

        Map<String, Object> probeRestCommandResponse;

        probeRestCommandResponse = executeProbeRestRequest();

        assertTrue(probeRestCommandResponse.get("response").equals("grid has started"));
        assertEquals(0, probeRestCommandResponse.get("successStatus"));
    }

    /**
     * This plugin awaits until it is given the signal to process -- thereby allowing an http request against a non
     * fully started kernal.
     */
    public static class DelayedStartPluginProvider extends AbstractTestPluginProvider {
        /** */
        private CountDownLatch triggerRestCmd;

        /** */
        private CountDownLatch triggerPluginStart;

        /** */
        public DelayedStartPluginProvider(CountDownLatch triggerPluginStartLatch,
            CountDownLatch triggerRestCmdLatch) {
            this.triggerPluginStart = triggerPluginStartLatch;
            this.triggerRestCmd = triggerRestCmdLatch;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "DelayedStartPlugin";
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() {
            super.onIgniteStart();

            triggerPluginStart.countDown();

            log.info("awaiting rest command latch ...");

            try {
                triggerRestCmd.await();
            }
            catch (InterruptedException e) {
                log.error("error in custom plugin", e);
            }

            log.info("finished awaiting rest command latch.");
        }
    }
}
