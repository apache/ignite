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

package org.apache.ignite.opencensus.spi.tracing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.Traces;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to check correctness of OpenCensus Tracing SPI implementation.
 */
public class OpenCensusTracingSpiTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Test test exporter handler. */
    private TraceExporterTestHandler hnd;

    /** Wrapper of test exporter handler. */
    private OpenCensusTraceExporter exporter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setTracingSpi(new OpenCensusTracingSpi());

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeTests() {
        /* Uncomment following code to see visualisation on local Zipkin: */

        //OpenCensusZipkinTraceExporter zipkinExproter = new OpenCensusZipkinTraceExporter(
        //    ZipkinExporterConfiguration.builder()
        //        .setV2Url("http://localhost:9411/api/v2/spans")
        //        .setServiceName("ignite")
        //        .build()
        //);
        //zipkinExproter.start(provider, "all");
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        hnd = new TraceExporterTestHandler();

        exporter = new OpenCensusTraceExporter(hnd);

        exporter.start("test");

        startGrids(GRID_CNT);
    }

    /**
     *
     */
    @After
    public void after() {
        exporter.stop();

        stopAllGrids();
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
            .stream().map(node -> (String) node.consistentId()).collect(Collectors.toList());

        hnd.flush();

        String joinedNodeId = joinedNode.localNode().id().toString();

        // Check existence of Traces.Discovery.NODE_JOIN_REQUEST spans with OK status on all nodes:
        Map<AttributeValue, SpanData> nodeJoinReqSpans = hnd.allSpans()
            .filter(span -> Traces.Discovery.NODE_JOIN_REQUEST.equals(span.getName()))
            .filter(span -> span.getStatus() == Status.OK)
            .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
            .collect(Collectors.toMap(
                span -> span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.NODE, SpanTags.NAME)),
                span -> span
                ));

        // NODE_JOIN_REQUEST must be processed at least on coordinator and joining node.
        // For other nodes there is no such guarantee.
        int CRD_IDX = 0;
        clusterNodeNames.stream().filter(
            node -> node.endsWith(String.valueOf(CRD_IDX)) || node.endsWith(String.valueOf(GRID_CNT))
        ).forEach(nodeName ->
            Assert.assertTrue(
                String.format(
                    "%s not found on node with name=%s, nodeJoinReqSpans=%s",
                    Traces.Discovery.NODE_JOIN_REQUEST, nodeName, nodeJoinReqSpans),
                nodeJoinReqSpans.containsKey(AttributeValue.stringAttributeValue(nodeName)))
        );

        // Check existence of Traces.Discovery.NODE_JOIN_ADD spans with OK status on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = hnd.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> Traces.Discovery.NODE_JOIN_ADD.equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found, nodeId=%d",
                    Traces.Discovery.NODE_JOIN_ADD, i),
                !nodeJoinReqSpans.isEmpty()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = hnd.spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid, parentSpan=" + parentSpan,
                    Traces.Discovery.NODE_JOIN_REQUEST,
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node, parentSpan=" + parentSpan,
                    AttributeValue.stringAttributeValue(joinedNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))
                );
            });
        }

        // Check existence of Traces.Discovery.NODE_JOIN_FINISH spans with OK status on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = hnd.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> Traces.Discovery.NODE_JOIN_FINISH.equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found, nodeId=%d",
                    Traces.Discovery.NODE_JOIN_FINISH, i),
                !nodeJoinReqSpans.isEmpty()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = hnd.spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    Traces.Discovery.NODE_JOIN_ADD,
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    AttributeValue.stringAttributeValue(joinedNodeId),
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
        List<String> clusterNodeNames = grid(0).cluster().nodes()
            .stream().map(node -> (String) node.consistentId()).collect(Collectors.toList());

        String leftNodeId = grid(GRID_CNT - 1).localNode().id().toString();

        stopGrid(GRID_CNT - 1);

        awaitPartitionMapExchange();

        hnd.flush();

        // Check existence of Traces.Discovery.NODE_LEFT spans with OK status on all nodes:
        Map<AttributeValue, SpanData> nodeLeftSpans = hnd.allSpans()
            .filter(span -> Traces.Discovery.NODE_LEFT.equals(span.getName()))
            .filter(span -> AttributeValue.stringAttributeValue(leftNodeId).equals(
                span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
            .filter(span -> span.getStatus() == Status.OK)
            .collect(Collectors.toMap(
                span -> span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.NODE, SpanTags.NAME)),
                span -> span,
                (span1, span2) -> {
                    throw new AssertionError(String.format(
                        "More than 1 %s span handled on a node (id can be extracted from span), " +
                            "existingSpan=%s, extraSpan=%s",
                        Traces.Discovery.NODE_LEFT, span1, span2
                    ));
                }
            ));

        clusterNodeNames.forEach(nodeName ->
            Assert.assertTrue(
                "Span " + Traces.Discovery.NODE_LEFT + " doesn't exist on node with name=" + nodeName,
                nodeLeftSpans.containsKey(AttributeValue.stringAttributeValue(nodeName)))
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

        hnd.flush();

        // Check PME for NODE_LEFT event on remaining nodes:
        for (int i = 0; i < GRID_CNT - 1; i++) {
            List<SpanData> exchFutSpans = hnd.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> Traces.Exchange.EXCHANGE_FUTURE.equals(span.getName()))
                .filter(span -> span.getStatus() == Status.OK)
                .filter(span -> AttributeValue.longAttributeValue(EventType.EVT_NODE_LEFT).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT, SpanTags.TYPE))))
                .filter(span -> AttributeValue.stringAttributeValue(leftNodeId).equals(
                    span.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))))
                .collect(Collectors.toList());

            Assert.assertTrue(
                String.format("%s span not found (or more than 1), nodeId=%d, exchFutSpans=%s",
                    Traces.Exchange.EXCHANGE_FUTURE, i, exchFutSpans),
                exchFutSpans.size() == 1
            );

            exchFutSpans.forEach(span -> {
                SpanData parentSpan = hnd.spanById(span.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + span,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    Traces.Discovery.NODE_LEFT,
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    AttributeValue.stringAttributeValue(leftNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(SpanTags.tag(SpanTags.EVENT_NODE, SpanTags.ID))
                );
                Assert.assertEquals(
                    "Exchange future major topology version is invalid " + span,
                    AttributeValue.longAttributeValue(curTopVer + 1),
                    span.getAttributes().getAttributeMap().get(
                        SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MAJOR))
                );
                Assert.assertEquals(
                    "Exchange future minor version is invalid " + span,
                    AttributeValue.longAttributeValue(0),
                    span.getAttributes().getAttributeMap().get(
                        SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MINOR))
                );
            });
        }
    }

    /**
     * Test span exporter handler.
     */
    static class TraceExporterTestHandler extends SpanExporter.Handler {
        /** Collected spans. */
        private final Map<SpanId, SpanData> collectedSpans = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void export(Collection<SpanData> spanDataList) {
            for (SpanData data : spanDataList)
                collectedSpans.put(data.getContext().getSpanId(), data);
        }

        /**
         * @return Stream of all exported spans.
         */
        public Stream<SpanData> allSpans() {
            return collectedSpans.values().stream();
        }

        /**
         * @param id Span id.
         * @return Exported span by given id.
         */
        public SpanData spanById(SpanId id) {
            return collectedSpans.get(id);
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @return Stream of all exported spans reported by node with given name.
         */
        public Stream<SpanData> spansReportedByNode(String igniteInstanceName) {
            return collectedSpans.values().stream()
                    .filter(spanData -> AttributeValue.stringAttributeValue(igniteInstanceName)
                        .equals(spanData.getAttributes().getAttributeMap().get("node.name")));
        }

        /**
         * Forces to flush ended spans that not passed to exporter yet.
         */
        public void flush() throws IgniteInterruptedCheckedException {
            // There is hardcoded invariant, that ended spans will be passed to exporter in 2 cases:
            // By 5 seconds timeout and if buffer size exceeds 32 spans.
            // There is no ability to change this behavior in Opencensus, so this hack is needed to "flush" real spans to exporter.
            // @see io.opencensus.implcore.trace.export.ExportComponentImpl.
            for (int i = 0; i < 32; i++) {
                Span span = Tracing.getTracer().spanBuilder("test-" + i).setSampler(Samplers.alwaysSample()).startSpan();

                U.sleep(10); // See same hack in OpenCensusSpanAdapter#end() method.

                span.end();
            }
        }
    }
}
