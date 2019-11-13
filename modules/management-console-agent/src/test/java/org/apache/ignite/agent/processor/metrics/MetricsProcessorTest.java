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

package org.apache.ignite.agent.processor.metrics;

import java.util.List;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsPullTopic;
import static org.apache.ignite.internal.IgniteFeatures.MANAGEMENT_CONSOLE;
import static org.apache.ignite.internal.IgniteFeatures.allFeatures;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;

/**
 * Metrics processor test.
 */
public class MetricsProcessorTest extends AgentCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldBeAvailableManagementConsoleFeature() throws Exception {
        startGrid(0);

        assertTrue(nodeSupports(allFeatures(grid(0).context()), MANAGEMENT_CONSOLE));
    }

    /**
     * Should send cluster metrics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldSendMetricsOnPull() throws Exception {
        IgniteEx ignite = startGrids(2);

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildMetricsPullTopic())
        );

        template.convertAndSend(buildMetricsPullTopic(), "pull");

        assertWithPoll(
            () -> {
                List<Object> metrics = interceptor.getAllRawPayloads(buildMetricsDest(cluster.id()));

                return metrics != null && metrics.size() == 2 && metrics.stream().allMatch(m -> new String((byte[]) m).contains(cluster.tag()));
            }
        );
    }
}
