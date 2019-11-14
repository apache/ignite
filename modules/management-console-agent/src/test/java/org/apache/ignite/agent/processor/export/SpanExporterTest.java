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

package org.apache.ignite.agent.processor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import com.google.common.collect.Lists;
import io.opencensus.common.Timestamp;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceId;
import io.opencensus.trace.Tracestate;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.agent.dto.tracing.SpanBatch;
import org.apache.ignite.agent.processor.AbstractServiceTest;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static io.opencensus.trace.TraceOptions.DEFAULT;
import static org.apache.ignite.agent.ManagementConsoleProcessor.TOPIC_MANAGEMENT_CONSOLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Span exporter test.
 */
public class SpanExporterTest extends AbstractServiceTest {
    /** Context. */
    private GridKernalContext ctx = getMockContext();

    /**
     * Should send traces to topic.
     */
    @Test
    public void shouldSendTracesToTopic() throws Exception {
        SpanExporter exporter = new SpanExporter(ctx);

        exporter.getTraceHandler().timeLimitedExport(getSpanData());

        ArgumentCaptor<Object> topicCaptor = ArgumentCaptor.forClass(Object.class);

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);

        verify(ctx.grid().message(), timeout(100).times(1)).send(topicCaptor.capture(), payloadCaptor.capture());

        Assert.assertEquals(TOPIC_MANAGEMENT_CONSOLE, topicCaptor.getValue());
        Assert.assertEquals(1, ((SpanBatch) payloadCaptor.getValue()).list().size());
    }

    /** {@inheritDoc} */
    @Override protected GridKernalContext getMockContext() {
        GridKernalContext ctx = super.getMockContext();

        IgniteClusterEx cluster = ctx.grid().cluster();

        ClusterGroup grp = mock(ClusterGroup.class);

        when(cluster.forServers()).thenReturn(grp);
        when(grp.nodes()).thenReturn(Lists.newArrayList(new TcpDiscoveryNode()));

        IgniteConfiguration cfg = mock(IgniteConfiguration.class);

        TracingSpi tracingSpi = mock(TracingSpi.class);

        when(ctx.config()).thenReturn(cfg);
        when(cfg.getTracingSpi()).thenReturn(tracingSpi);

        return ctx;
    }

    /**
     * @return Span data list.
     */
    private List<SpanData> getSpanData() {
        return Lists.newArrayList(
            SpanData.create(
                SpanContext.create(TraceId.generateRandomId(new Random()), SpanId.generateRandomId(new Random()), DEFAULT, Tracestate.builder().build()),
                SpanId.generateRandomId(new Random()),
                false,
                "name",
                null,
                Timestamp.create(10, 10),
                SpanData.Attributes.create(new HashMap<>(), 0),
                SpanData.TimedEvents.create(new ArrayList<>(), 0),
                SpanData.TimedEvents.create(new ArrayList<>(), 0),
                SpanData.Links.create(new ArrayList<>(), 0),
                null,
                null,
                Timestamp.create(20, 20)
            )
        );
    }
}
