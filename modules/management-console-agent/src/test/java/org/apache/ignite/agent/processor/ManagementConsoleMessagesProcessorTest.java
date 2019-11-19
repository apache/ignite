/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor;

import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.agent.dto.tracing.Span;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static java.lang.String.valueOf;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildEventsDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Management console messages processor test.
 */
public class ManagementConsoleMessagesProcessorTest extends AgentCommonAbstractTest {
    /**
     * Should register handler and export events.
     */
    @Test
    public void shouldSendNodeJoinEvent() throws Exception {
        IgniteEx ignite = startGrid(0);
        changeManagementConsoleUri(ignite);

        startGrid(1);

        IgniteCluster cluster = ignite.cluster();

        assertWithPoll(() -> {
            List<JsonNode> evts = interceptor.getListPayload(buildEventsDest(cluster.id()), JsonNode.class);

            if (evts != null && evts.size() == 1) {
                JsonNode evt = F.first(evts);

                return  EVT_NODE_JOINED == evt.get("typeId").asInt();
            }

            return false;
        });
    }

    /**
     * Should register handler and export events.
     */
    @Test
    public void shouldSendActivationEvent() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> {
            List<JsonNode> evts = interceptor.getListPayload(buildEventsDest(cluster.id()), JsonNode.class);

            if (evts != null && evts.size() == 1) {
                JsonNode evt = F.first(evts);

                return  EVT_CLUSTER_ACTIVATED == evt.get("typeId").asInt();
            }

            return false;
        });
    }

    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialSpans() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildSaveSpanDest(cluster.id())) != null);
    }

    /**
     * Should send spans.
     */
    @Test
    public void shouldSendSpans() throws Exception {
        IgniteEx ignite_1 = startGrid(0);

        changeManagementConsoleUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();

        cluster.active(true);

        assertWithPoll(
            () -> {
                List<Span> spans = interceptor.getListPayload(buildSaveSpanDest(cluster.id()), Span.class);

                return spans != null && !spans.isEmpty();
            }
        );
    }

    /**
     * Should send node configuration.
     */
    @Test
    public void shouldSendNodeConfiguration() throws Exception {
        IgniteEx ignite_1 = startGrid(0);

        changeManagementConsoleUri(ignite_1);

        IgniteCluster cluster = ignite_1.cluster();

        cluster.active(true);

        assertWithPoll(
            () -> {
                List<NodeConfiguration> cfg = interceptor.getListPayload(buildClusterNodeConfigurationDest(cluster.id()), NodeConfiguration.class);

                return cfg != null
                    && cfg.size() == 1
                    && cfg.get(0).getConsistentId().equals(valueOf(cluster.localNode().consistentId()));
            }
        );
    }
}
