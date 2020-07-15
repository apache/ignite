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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Status;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.Assert;
import org.junit.Test;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_JOB_EXECUTE_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_JOB_EXECUTE_RESPONSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_REGULAR_PROCESS;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_READ;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_WRITE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CUSTOM_JOB_CALL;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_CUSTOM_EVENT;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_JOIN_ADD;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_JOIN_FINISH;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_JOIN_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_LEFT;
import static org.apache.ignite.internal.processors.tracing.SpanType.EXCHANGE_FUTURE;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Tests to check correctness of OpenCensus Tracing SPI implementation.
 */
public class OpenCensusTracingSpiTest extends AbstractTracingTest {
    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.DISCOVERY).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.COMMUNICATION).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * Test checks that node join process is traced correctly in positive case.
     */
    @Test
    public void testNodeJoinTracing() throws Exception {
        IgniteEx joinedNode = startGrid(GRID_CNT);

        awaitPartitionMapExchange();

        // Consistent id is the same with node name.
        List<String> clusterNodeNames = grid(0).cluster().nodes()
            .stream().map(node -> (String)node.consistentId()).collect(Collectors.toList());

        handler().flush();

        String joinedNodeId = joinedNode.localNode().id().toString();

        // Check existence of Traces.Discovery.NODE_JOIN_REQUEST spans with OK status on all nodes:
        Map<AttributeValue, SpanData> nodeJoinReqSpans = handler().allSpans()
            .filter(span -> DISCOVERY_NODE_JOIN_REQUEST.spanName().equals(span.getName()))
            .filter(span -> span.getStatus() == Status.OK)
            .filter(span -> stringAttributeValue(joinedNodeId).equals(
                span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
            .collect(Collectors.toMap(
                span -> span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.NODE, SpanTags.NAME)),
                span -> span
            ));

        // NODE_JOIN_REQUEST must be processed at least on coordinator node.
        // For other nodes there is no such guarantee.
        int CRD_IDX = 0;
        clusterNodeNames.stream().filter(
            node -> node.endsWith(String.valueOf(CRD_IDX))
        ).forEach(nodeName ->
            Assert.assertTrue(
                String.format(
                    "%s not found on node with name=%s, nodeJoinReqSpans=%s",
                    DISCOVERY_NODE_JOIN_REQUEST, nodeName, nodeJoinReqSpans),
                nodeJoinReqSpans.containsKey(stringAttributeValue(nodeName)))
        );

        // Check existence of Traces.Discovery.NODE_JOIN_ADD spans with OK status on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = handler().spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> DISCOVERY_NODE_JOIN_ADD.spanName().equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found, nodeId=%d",
                    DISCOVERY_NODE_JOIN_ADD, i),
                !nodeJoinReqSpans.isEmpty()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = handler().spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid, parentSpan=" + parentSpan,
                    DISCOVERY_NODE_JOIN_REQUEST.spanName(),
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node, parentSpan=" + parentSpan,
                    stringAttributeValue(joinedNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))
                );
            });
        }

        // Check existence of Traces.Discovery.NODE_JOIN_FINISH spans with OK status on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = handler().spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> DISCOVERY_NODE_JOIN_FINISH.spanName().equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found, nodeId=%d",
                    DISCOVERY_NODE_JOIN_FINISH, i),
                !nodeJoinReqSpans.isEmpty()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = handler().spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    DISCOVERY_NODE_JOIN_ADD.spanName(),
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    stringAttributeValue(joinedNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))
                );
            });
        }
    }

    /**
     * Test checks that node left process is traced correctly in positive case.
     */
    @Test
    public void testNodeLeftTracing() throws Exception {
        // Consistent id is the same with node name.
        List<String> clusterNodeNames = grid(0).cluster().forServers().nodes()
            .stream().map(node -> (String)node.consistentId()).collect(Collectors.toList());

        String leftNodeId = grid(GRID_CNT - 1).localNode().id().toString();

        stopGrid(GRID_CNT - 1);

        awaitPartitionMapExchange();

        handler().flush();

        // Check existence of DISCOVERY_NODE_LEFT spans with OK status on all nodes:
        Map<AttributeValue, SpanData> nodeLeftSpans = handler().allSpans()
            .filter(span -> DISCOVERY_NODE_LEFT.spanName().equals(span.getName()))
            .filter(span -> stringAttributeValue(leftNodeId).equals(
                span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
            .filter(span -> span.getStatus() == Status.OK)
            .collect(Collectors.toMap(
                span -> span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.NODE, SpanTags.NAME)),
                span -> span,
                (span1, span2) -> {
                    throw new AssertionError(String.format(
                        "More than 1 %s span handled on a node (id can be extracted from span), " +
                            "existingSpan=%s, extraSpan=%s",
                        DISCOVERY_NODE_LEFT, span1, span2
                    ));
                }
            ));

        clusterNodeNames.forEach(nodeName ->
            Assert.assertTrue(
                "Span " + DISCOVERY_NODE_LEFT + " doesn't exist on node with name=" + nodeName,
                nodeLeftSpans.containsKey(stringAttributeValue(nodeName)))
        );
    }

    /**
     * Test checks that PME process in case of node left discovery event is traced correctly in positive case.
     */
    @Test
    public void testPartitionsMapExchangeTracing() throws Exception {
        long curTopVer = grid(0).cluster().topologyVersion();

        String leftNodeId = grid(GRID_CNT - 1).localNode().id().toString();

        stopGrid(GRID_CNT - 1);

        awaitPartitionMapExchange();

        handler().flush();

        // Check PME for NODE_LEFT event on remaining nodes:
        for (int i = 0; i < GRID_CNT - 1; i++) {
            List<SpanData> exchFutSpans = handler().spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> EXCHANGE_FUTURE.spanName().equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> AttributeValue.stringAttributeValue(String.valueOf(EventType.EVT_NODE_LEFT)).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT, SpanTags.TYPE))))
                .filter(span -> stringAttributeValue(leftNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found (or more than 1), nodeId=%d, exchFutSpans=%s",
                    EXCHANGE_FUTURE, i, exchFutSpans),
                exchFutSpans.size() == 1
            );

            exchFutSpans.forEach(span -> {
                SpanData parentSpan = handler().spanById(span.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + span,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    DISCOVERY_NODE_LEFT.spanName(),
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    stringAttributeValue(leftNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))
                );
                Assert.assertEquals(
                    "Exchange future major topology version is invalid " + span,
                    AttributeValue.stringAttributeValue(String.valueOf(curTopVer + 1)),
                    span.getAttributes().getAttributeMap().get(
                        SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MAJOR))
                );
                Assert.assertEquals(
                    "Exchange future minor version is invalid " + span,
                    AttributeValue.stringAttributeValue("0"),
                    span.getAttributes().getAttributeMap().get(
                        SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MINOR))
                );
            });
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCommunicationMessages() throws Exception {
        IgniteEx ignite = grid(0);
        IgniteEx ignite1 = grid(1);

        try (MTC.TraceSurroundings ignore = MTC.support(ignite.context().tracing().create(CUSTOM_JOB_CALL))) {
            ignite.compute(ignite.cluster().forNode(ignite1.localNode())).withNoFailover().call(() -> "");
        }

        handler().flush();

        SpanData jobSpan = handler().spanByName(CUSTOM_JOB_CALL.spanName());

        List<SpanData> data = handler().unrollByParent(jobSpan);

        List<AttributeValue> nodejobMsgTags = data.stream()
            .filter(it -> it.getAttributes().getAttributeMap().containsKey(SpanTags.MESSAGE))
            .map(it -> it.getAttributes().getAttributeMap().get(SpanTags.MESSAGE))
            .collect(Collectors.toList());

        List<String> nodejobTraces = data.stream()
            .map(SpanData::getName)
            .collect(Collectors.toList());

        assertEquals(nodejobTraces.toString(), 7, nodejobTraces.size());

        assertEquals(1, nodejobTraces.stream().filter(it -> it.contains(CUSTOM_JOB_CALL.spanName())).count());

        //request + response
        assertEquals(2, nodejobTraces.stream().filter(it -> it.contains(COMMUNICATION_SOCKET_WRITE.spanName())).count());
        //request + response
        assertEquals(2, nodejobTraces.stream().filter(it -> it.contains(COMMUNICATION_SOCKET_READ.spanName())).count());
        //request + response
        assertEquals(2, nodejobTraces.stream().filter(it -> it.contains(COMMUNICATION_REGULAR_PROCESS.spanName())).count());

        assertTrue(nodejobMsgTags.stream().anyMatch(it -> it.equals(stringAttributeValue(COMMUNICATION_JOB_EXECUTE_REQUEST.spanName()))));
        assertTrue(nodejobMsgTags.stream().anyMatch(it -> it.equals(stringAttributeValue(COMMUNICATION_JOB_EXECUTE_RESPONSE.spanName()))));
    }

    /**
     */
    @Test
    public void testTracingFeatureAvailable() {
        assertTrue(IgniteFeatures.nodeSupports(IgniteFeatures.allFeatures(), IgniteFeatures.TRACING));
    }

    /**
     * Ensure that root discovery.custom.event have message.class with corresponding value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEventContainsMessageClassTag() throws Exception {
        IgniteEx ignite = grid(0);

        startGrid(GRID_CNT).createCache("New cache");

        handler().flush();

        // Only root discovery.custom.event spans have message.class tag.
        List<SpanData> rootCustomEvtSpans = handler().allSpans().
            filter(spanData ->
                DISCOVERY_CUSTOM_EVENT.spanName().equals(spanData.getName()) &&
                    spanData.getParentSpanId() == null).
            collect(Collectors.toList());

        // Check that there's at least one discovery.custom.event span with tag "message.class"
        // and value "CacheAffinityChangeMessage"
        assertTrue(rootCustomEvtSpans.stream().anyMatch(
            span -> "CacheAffinityChangeMessage".equals(
                attributeValueToString(span.getAttributes().getAttributeMap().get(SpanTags.MESSAGE_CLASS)))));
    }
}
