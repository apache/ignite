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

package org.apache.ignite.spi.tracing.opencensus;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.internal.tracing.TracingSpiType;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing SPI implementation based on OpenCensus library.
 *
 * If you have OpenCensus Tracing in your environment use the following code for configuration:
 * <code>
 *     IgniteConfiguration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi());
 * </code>
 * If you don't have OpenCensus Tracing:
 * <code>
 *     IgniteConfiguration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi(new ZipkinExporterHandler(...)));
 * </code>
 *
 * See constructors description for detailed explanation.
 */
@IgniteSpiMultipleInstancesSupport(value = true)
@IgniteSpiConsistencyChecked(optional = true)
public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi<OpenCensusSpanAdapter> {
    /** Configured exporters. */
    private final List<OpenCensusTraceExporter> exporters;

    /** Flag indicates that external Tracing is used in environment. In this case no exporters will be started. */
    private final boolean externalProvider;

    /**
     * This constructor is used if environment (JVM) already has OpenCensus tracing.
     * In this case traces from the node will go trough externally registered exporters by an user himself.
     *
     * @see Tracing#getExportComponent()
     */
    public OpenCensusTracingSpi() {
        exporters = null;

        externalProvider = true;
    }

    /**
     * This constructor is used if environment (JVM) hasn't OpenCensus tracing.
     * In this case provided exporters will start and traces from the node will go through it.
     *
     * @param exporters Exporters.
     */
    public OpenCensusTracingSpi(SpanExporter.Handler... exporters) {
        this.exporters = Arrays.stream(exporters).map(OpenCensusTraceExporter::new).collect(Collectors.toList());

        externalProvider = false;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(
        @NotNull String name,
        @Nullable byte[] parentSerializedSpan
    ) throws Exception
    {
        return new OpenCensusSpanAdapter(
            Tracing.getTracer().spanBuilderWithRemoteParent(
                name,
                Tracing.getPropagationComponent().getBinaryFormat().fromByteArray(parentSerializedSpan)
            )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
        );
    }

    /** {@inheritDoc} */
    @Override public @NotNull OpenCensusSpanAdapter create(
        @NotNull String name,
        @Nullable OpenCensusSpanAdapter parentSpan) {
        try {
            io.opencensus.trace.Span openCensusParent = null;

            if (parentSpan != null)
                openCensusParent = parentSpan.impl();

            return new OpenCensusSpanAdapter(
                Tracing.getTracer().spanBuilderWithExplicitParent(
                    name,
                    openCensusParent
                )
                    .setSampler(Samplers.alwaysSample())
                    .startSpan()
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create span from parent " +
                "[spanName=" + name + ", parentSpan=" + parentSpan + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull OpenCensusSpanAdapter span) {
        return Tracing.getPropagationComponent().getBinaryFormat().toByteArray(span.impl().getContext());
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.start(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.stop();
    }

    /** {@inheritDoc} */
    @Override public byte type() {
        return TracingSpiType.OPEN_CENSUS_TRACING_SPI.index();
    }
}
