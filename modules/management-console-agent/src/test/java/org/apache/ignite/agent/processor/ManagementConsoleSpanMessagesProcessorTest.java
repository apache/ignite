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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.tracing.Span;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.agent.StompDestinationsUtils.buildSaveSpanDest;
import static org.awaitility.Awaitility.with;

/**
 * Tests for Control Center span messages processor.
 */
public class ManagementConsoleSpanMessagesProcessorTest extends AgentCommonAbstractTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialSpans() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleConfig(ignite);

        IgniteClusterEx cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildSaveSpanDest(cluster.id())) != null);
    }

    /**
     * GG-26201 - Testcase 1:
     *
     * 1. Start cluster of 2 nodes, each supports tracing
     * 2. Verify the tracing works - messages from both nodes are sent to GMC
     */
    @Test
    public void shouldSendSpansFromTwoNodes() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteClusterEx cluster = ignite.cluster();

        IgniteEx ignite_2 = startGrid(1);

        changeManagementConsoleConfig(ignite);

        assertWithPoll(() -> {
            List<Span> spans = getAllSpans(cluster.id());

            return !spans.isEmpty() &&
                hasSpansFromSpecificNode(spans, ignite.localNode().id()) &&
                hasSpansFromSpecificNode(spans, ignite_2.localNode().id());
        });
    }

    /**
     * GG-26201 - Testcase 2:
     *
     * 1. Start a node with tracing
     * 2. Start a node without tracing
     * 3. Verify the messages from 1st node were sent to GMC
     */
    @Test
    public void shouldSendSpansFromFirstNodeWithTracing_And_NoSpansFromSecondNodeWithDisabledTracing() throws Exception {
        IgniteEx ignite = startGrid(0);

        changeManagementConsoleConfig(ignite);

        IgniteClusterEx cluster = ignite.cluster();

        IgniteEx ignite_2 = (IgniteEx) startGrid(getTestIgniteInstanceName(1) + "without-tracing", getIgniteConfigurationWithoutTracing());

        assertWithPoll(() -> {
            List<Span> spans = getAllSpans(cluster.id());

            return !spans.isEmpty() &&
                hasSpansFromSpecificNode(spans, ignite.localNode().id()) &&
                !hasSpansFromSpecificNode(spans, ignite_2.localNode().id());
        });
    }

    /**
     * GG-26201 - Testcase 3:
     *
     * 1. Start a node without tracing
     * 2. Start a node with tracing
     * 3. Verify the messages from 2nd node were sent to GMC
     */
    @Test
    public void shouldSendSpansFromSecondNodeWithTracing_And_NoSpansFromFirstNodeWithDisabledTracing()
        throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid(getTestIgniteInstanceName(0) + "without-tracing",
            getIgniteConfigurationWithoutTracing());

        changeManagementConsoleConfig(ignite);

        IgniteClusterEx cluster = ignite.cluster();

        IgniteEx ignite_2 = startGrid(1);

        assertWithPoll(() -> {
            List<Span> spans = getAllSpans(cluster.id());

            return !spans.isEmpty() &&
                !hasSpansFromSpecificNode(spans, ignite.localNode().id()) &&
                hasSpansFromSpecificNode(spans, ignite_2.localNode().id());
        });
    }

    /**
     * GG-26201 - Testcase 4:
     *
     * 1. Start 2 nodes without tracing
     * 2. Verify no messages to GMC after 2 second after start
     * 3. Start 3rd node with tracing
     * 4. Verify the messages from 3rd node sent to GMC
     * 5. Stop 3rd node
     * 6. Verify no new messages sent to GMC during 2 seconds
     * 7. Start new node without tracing
     * 8. Verify no new messages sent to GMC during 2 seconds
     */
    @Test
    public void shouldNotSendSpansWithDisabledTracingOnAllNodes() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid(getTestIgniteInstanceName(0) + "without-tracing",
            getIgniteConfigurationWithoutTracing());

        changeManagementConsoleConfig(ignite);

        IgniteClusterEx cluster = ignite.cluster();

        IgniteEx ignite_2 = (IgniteEx) startGrid(getTestIgniteInstanceName(1) + "without-tracing",
            getIgniteConfigurationWithoutTracing());

        assertWithPoll(spanListIsEmpty(cluster.id()));

        IgniteEx ignite_3 = startGrid(2);

        assertWithPoll(() -> {
            List<Span> spans = getAllSpans(cluster.id());

            return !spans.isEmpty() &&
                !hasSpansFromSpecificNode(spans, ignite.localNode().id()) &&
                !hasSpansFromSpecificNode(spans, ignite_2.localNode().id()) &&
                hasSpansFromSpecificNode(spans, ignite_3.localNode().id());
        });

        stopGrid(ignite_3.name(), true);

        Thread.sleep(6000);

        interceptor.clearMessages();

        assertWithPoll(spanListIsEmpty(cluster.id()));

        IgniteEx ignite_4 = (IgniteEx) startGrid(getTestIgniteInstanceName(3) + "without-tracing", getIgniteConfigurationWithoutTracing());

        assertWithPoll(spanListIsEmpty(cluster.id()));
    }

    /** {@inheritDoc} */
    @Override protected void assertWithPoll(Callable<Boolean> cond) {
        with().pollInterval(2, SECONDS).await().atMost(8, SECONDS).until(cond);
    }

    /**
     * @param cluisterId Cluister id.
     * @return All spans by cluster id.
     */
    private List<Span> getAllSpans(UUID cluisterId) {
        return interceptor.getAllListPayloads(buildSaveSpanDest(cluisterId), Span.class)
            .stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * @param clusterId Cluster id.
     * @return Callable function whick chech that span list is empty.
     */
    private Callable<Boolean> spanListIsEmpty(UUID clusterId) {
        return () -> getAllSpans(clusterId).isEmpty();
    }

    /**
     * @param spans Spans.
     * @param nid Node ID.
     * @return {@code True} if span list has spans from specific node.
     */
    private boolean hasSpansFromSpecificNode(List<Span> spans, UUID nid) {
        return spans.stream().anyMatch(s -> s.getTags().get("node.id").equals(nid.toString()));
    }

    /**
     * @return Ignite configuration with disabled tracing.
     */
    private IgniteConfiguration getIgniteConfigurationWithoutTracing() {
        return new IgniteConfiguration()
            .setLocalHost("127.0.0.1")
            .setAuthenticationEnabled(false)
            .setMetricsLogFrequency(0)
            .setQueryThreadPoolSize(16)
            .setFailureDetectionTimeout(10000)
            .setClientFailureDetectionTimeout(10000)
            .setNetworkTimeout(10000)
            .setConnectorConfiguration(null)
            .setClientConnectorConfiguration(null)
            .setTransactionConfiguration(
                new TransactionConfiguration()
                    .setTxTimeoutOnPartitionMapExchange(60 * 1000)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setFailureHandler(new NoOpFailureHandler())
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder()
                            .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                    )
            );
    }
}
