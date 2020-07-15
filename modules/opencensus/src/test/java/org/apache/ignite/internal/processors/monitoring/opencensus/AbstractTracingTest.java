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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.opencensus.common.Functions;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTraceExporter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.EXCHANGE;
import static org.apache.ignite.spi.tracing.Scope.TX;

/**
 * Abstract class for open census tracing tests.
 */
public abstract class AbstractTracingTest extends GridCommonAbstractTest {
    /** Grid count. */
    static final int GRID_CNT = 3;

    /** Span buffer count - hardcode in open census. */
    private static final int SPAN_BUFFER_COUNT = 32;

    /** Default configuration map. */
    static final Map<TracingConfigurationCoordinates, TracingConfigurationParameters> DFLT_CONFIG_MAP =
        new HashMap<>();

    /** TX scope specific coordinates to be used within several tests. */
    static final TracingConfigurationCoordinates TX_SCOPE_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(TX).build();

    /** EXCHANGE scope specific coordinates to be used within several tests. */
    static final TracingConfigurationCoordinates EXCHANGE_SCOPE_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(EXCHANGE).build();

    /** Updated scope specific parameters to be used within several tests. */
    static final TracingConfigurationParameters SOME_SCOPE_SPECIFIC_PARAMETERS =
        new TracingConfigurationParameters.Builder().withSamplingRate(0.75).
            withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

    /** TX Label specific coordinates to be used within several tests. */
    static final TracingConfigurationCoordinates TX_LABEL_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

    /** Updated label specific parameters to be used within several tests. */
    static final TracingConfigurationParameters SOME_LABEL_SPECIFIC_PARAMETERS =
        new TracingConfigurationParameters.Builder().withSamplingRate(0.111).
            withIncludedScopes(Collections.singleton(EXCHANGE)).build();

    static {
        DFLT_CONFIG_MAP.put(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION);

        DFLT_CONFIG_MAP.put(
            new TracingConfigurationCoordinates.Builder(Scope.COMMUNICATION).build(),
            TracingConfigurationManager.DEFAULT_COMMUNICATION_CONFIGURATION);

        DFLT_CONFIG_MAP.put(
            new TracingConfigurationCoordinates.Builder(Scope.EXCHANGE).build(),
            TracingConfigurationManager.DEFAULT_EXCHANGE_CONFIGURATION);

        DFLT_CONFIG_MAP.put(
            new TracingConfigurationCoordinates.Builder(Scope.DISCOVERY).build(),
            TracingConfigurationManager.DEFAULT_DISCOVERY_CONFIGURATION);
    }

    /** Test trace exporter handler. */
    private OpenCensusTxTracingTest.TraceExporterTestHandler hnd;

    /** Wrapper of test exporter handler. */
    private OpenCensusTraceExporter exporter;

    /**
     * @return Tracing SPI to be used within tests.
     */
    protected abstract TracingSpi getTracingSpi();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        cfg.setTracingSpi(getTracingSpi());

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeTests() {
        /* Uncomment following code to see visualisation on local Zipkin: */

//        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
//            .setV2Url("http://localhost:9411/api/v2/spans")
//            .setServiceName("ignite")
//            .build());
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        hnd = new OpenCensusTxTracingTest.TraceExporterTestHandler();

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
     * @return Handler.
     */
    OpenCensusTxTracingTest.TraceExporterTestHandler handler() {
        return hnd;
    }

    /**
     * Check span.
     *
     * @param spanType Span type.
     * @param parentSpanId Parent span id.
     * @param expSpansCnt expected spans count.
     * @param expAttrs Attributes to check.
     * @return List of founded span ids.
     */
    java.util.List<SpanId> checkSpan(
        SpanType spanType,
        SpanId parentSpanId,
        int expSpansCnt,
        /* tagName: tagValue*/ Map<String, String> expAttrs
    ) {
        java.util.List<SpanData> gotSpans = hnd.allSpans()
            .filter(
                span -> parentSpanId != null ?
                    parentSpanId.equals(span.getParentSpanId()) && spanType.spanName().equals(span.getName()) :
                    spanType.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(expSpansCnt, gotSpans.size());

        java.util.List<SpanId> spanIds = new ArrayList<>();

        gotSpans.forEach(spanData -> {
            spanIds.add(spanData.getContext().getSpanId());

            checkSpanAttributes(spanData, expAttrs);
        });

        return spanIds;
    }

    /**
     * Verify that given spanData contains all (and only) propagated expected attributes.
     * @param spanData Span data to check.
     * @param expAttrs Attributes to check.
     */
    private void checkSpanAttributes(SpanData spanData, /* tagName: tagValue*/ Map<String, String> expAttrs) {
        Map<String, AttributeValue> attrs = spanData.getAttributes().getAttributeMap();

        if (expAttrs != null) {
            assertEquals(expAttrs.size(), attrs.size());

            for (Map.Entry<String, String> entry : expAttrs.entrySet())
                assertEquals(entry.getValue(), attributeValueToString(attrs.get(entry.getKey())));
        }
    }

    /**
     * @param attributeVal Attribute value.
     */
    protected static String attributeValueToString(AttributeValue attributeVal) {
        if (attributeVal == null)
            return null;

        return attributeVal.match(
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnConstant(""));
    }

    /**
     * Test span exporter handler.
     */
    static class TraceExporterTestHandler extends SpanExporter.Handler {
        /** Collected spans. */
        private final Map<SpanId, SpanData> collectedSpans = new ConcurrentHashMap<>();

        /** */
        private final Map<SpanId, java.util.List<SpanData>> collectedSpansByParents = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void export(Collection<SpanData> spanDataList) {
            for (SpanData data : spanDataList) {
                collectedSpans.put(data.getContext().getSpanId(), data);

                if (data.getParentSpanId() != null)
                    collectedSpansByParents.computeIfAbsent(data.getParentSpanId(), (k) -> new ArrayList<>()).add(data);
            }
        }

        /**
         * @return Stream of all exported spans.
         */
        Stream<SpanData> allSpans() {
            return collectedSpans.values().stream();
        }

        /**
         * @param id Span id.
         * @return Exported span by given id.
         */
        SpanData spanById(SpanId id) {
            return collectedSpans.get(id);
        }

        /**
         * @param name Span name for search.
         * @return Span with given name.
         */
        SpanData spanByName(String name) {
            return allSpans()
                .filter(span -> span.getName().contains(name))
                .findFirst()
                .orElse(null);
        }

        /**
         * @param parentId Parent id.
         * @return All spans by parent id.
         */
        java.util.List<SpanData> spanByParentId(SpanId parentId) {
            return collectedSpansByParents.get(parentId);
        }

        /**
         * @param parentSpan Top span.
         * @return All span which are child of parentSpan in any generation.
         */
        java.util.List<SpanData> unrollByParent(SpanData parentSpan) {
            ArrayList<SpanData> spanChain = new ArrayList<>();

            LinkedList<SpanData> queue = new LinkedList<>();

            queue.add(parentSpan);

            spanChain.add(parentSpan);

            while (!queue.isEmpty()) {
                SpanData cur = queue.pollFirst();

                assert cur != null;

                List<SpanData> child = spanByParentId(cur.getContext().getSpanId());

                if (child != null) {
                    spanChain.addAll(child);

                    queue.addAll(child);
                }
            }

            return spanChain;
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @return Stream of SpanData.
         */
        Stream<SpanData> spansReportedByNode(String igniteInstanceName) {
            return collectedSpans.values().stream()
                .filter(spanData -> stringAttributeValue(igniteInstanceName)
                    .equals(spanData.getAttributes().getAttributeMap().get("node.name")));
        }

        /**
         * Forces to flush ended spans that not passed to exporter yet.
         */
        void flush() throws IgniteInterruptedCheckedException {
            // There is hardcoded invariant, that ended spans will be passed to exporter in 2 cases:
            // By 5 seconds timeout and if buffer size exceeds 32 spans.
            // There is no ability to change this behavior in Opencensus, so this hack is needed to "flush" real spans to exporter.
            // @see io.opencensus.implcore.trace.export.ExportComponentImpl.
            for (int i = 0; i < SPAN_BUFFER_COUNT; i++) {
                Span span = Tracing.getTracer().spanBuilder("test-" + i).setSampler(Samplers.alwaysSample()).startSpan();

                U.sleep(10); // See same hack in OpenCensusSpanAdapter#end() method.

                span.end();
            }
        }
    }
}
