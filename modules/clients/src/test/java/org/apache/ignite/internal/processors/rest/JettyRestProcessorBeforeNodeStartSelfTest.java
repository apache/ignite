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

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpStrategy;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpTestPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.NODE_STATE_BEFORE_START;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.WARM_UP;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * A class for testing query execution before the node starts.
 */
public class JettyRestProcessorBeforeNodeStartSelfTest extends JettyRestProcessorCommonSelfTest {
    /** Flag for use next port for jetty. */
    boolean nextPort;

    /** {@inheritDoc} */
    @Override protected String signature() throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int restPort() {
        return super.restPort() + (nextPort ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new WarmUpTestPluginProvider())
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            );
    }

    /**
     * Test verifies that the requests will not be completed successfully because the node has already started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testErrorExecuteCommandAfterNodeStart() throws Exception {
        for (GridRestCommand cmd : new GridRestCommand[] {NODE_STATE_BEFORE_START, WARM_UP})
            assertEquals("Node has already started.", content(cmd, null).get("error").asText());
    }

    /**
     * Test checks that the warm-up will be stopped via an http request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopWarmUp() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(gridCount()));

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()
            .setWarmUpConfiguration(new BlockedWarmUpConfiguration());

        WarmUpTestPluginProvider warmUpProvider = (WarmUpTestPluginProvider)cfg.getPluginProviders()[0];
        BlockedWarmUpStrategy warmUpStrat = (BlockedWarmUpStrategy)warmUpProvider.strats.get(1);

        nextPort = true;

        IgniteInternalFuture<IgniteEx> startNodeFut = GridTestUtils.runAsync(() -> startGrid(cfg));

        try {
            U.await(warmUpStrat.startLatch, 1, MINUTES);

            JsonNode res = content(NODE_STATE_BEFORE_START, null);

            assertEquals(STATUS_SUCCESS, res.get("successStatus").intValue());

            res = content(WARM_UP, F.asMap("stopWarmUp", "true"));

            assertEquals(STATUS_SUCCESS, res.get("successStatus").intValue());
            assertEquals(0, warmUpStrat.stopLatch.getCount());
        }
        finally {
            warmUpStrat.stopLatch.countDown();

            startNodeFut.get(1, MINUTES).close();
        }
    }

    /**
     * Execute REST command and return result.
     *
     * @param cmd Command.
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    private JsonNode content(GridRestCommand cmd, @Nullable Map<String, String> params) throws Exception {
        Map<String, String> p = new HashMap<>();

        p.put("cmd", cmd.key());

        if (nonNull(params))
            p.putAll(params);

        return JSON_MAPPER.readTree(content(p));
    }
}
