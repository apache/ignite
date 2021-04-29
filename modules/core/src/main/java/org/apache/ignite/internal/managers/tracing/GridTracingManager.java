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

package org.apache.ignite.internal.managers.tracing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.tracing.DeferredSpan;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanImpl;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.processors.tracing.configuration.GridTracingConfigurationManager;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.tracing.NoopTracingSpi;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpiSpecificSpan;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToShort;
import static org.apache.ignite.internal.util.GridClientByteUtils.intToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.shortToBytes;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Tracing Manager.
 */
public class GridTracingManager extends GridManagerAdapter<TracingSpi> implements Tracing {
    /** */
    private static final int SPECIAL_FLAGS_OFF = 0;

    /** */
    private static final int SPI_TYPE_OFF = SPECIAL_FLAGS_OFF + 1;

    /** */
    private static final int MAJOR_PROTOCOL_VERSION_OFF = SPI_TYPE_OFF + 1;

    /** */
    private static final int MINOR_PROTOCOL_VERSION_OFF = MAJOR_PROTOCOL_VERSION_OFF + 1;

    /** */
    private static final int SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH_OFF = MINOR_PROTOCOL_VERSION_OFF + 1;

    /** */
    private static final int SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF = SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH_OFF + 4;

    /** */
    private static final int PARENT_SPAN_TYPE_BYTES_LENGTH = 4;

    /** */
    private static final int INCLUDED_SCOPES_SIZE_BYTE_LENGTH = 4;

    /** */
    private static final int SCOPE_INDEX_BYTE_LENGTH = 2;

    /** */
    private static final int SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH = 4;

    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /** Tracing configuration */
    private final TracingConfigurationManager tracingConfiguration;

    /**
     * Major span serialization protocol version.
     * Within same major protocol version span serialization should be backward compatible.
     */
    private static final byte MAJOR_PROTOCOL_VERSION = 0;

    /** Minor span serialization protocol version. */
    private static final byte MINOR_PROTOCOL_VERSION = 0;

    /** Noop traceable message handler. */
    private static final TraceableMessagesHandler NOOP_TRACEABLE_MSG_HANDLER =
        new TraceableMessagesHandler(new NoopTracing(), new NullLogger());

    /** Flag that indicates that noop tracing spi is used. */
    private boolean noop = true;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param useNoopTracingSpi Flag that signals that NoOp tracing spi should be used instead of the one,
     * specified in the context. It's a part of the failover logic that is suitable if an exception is thrown
     * when the manager starts.
     */
    public GridTracingManager(GridKernalContext ctx, boolean useNoopTracingSpi) {
        super(ctx, useNoopTracingSpi ? new NoopTracingSpi() : ctx.config().getTracingSpi());

        msgHnd = new TraceableMessagesHandler(this, ctx.log(GridTracingManager.class));

        tracingConfiguration = new GridTracingConfigurationManager(ctx);
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void start() throws IgniteCheckedException {
        try {
            startSpi();

            noop = getSpi() instanceof NoopTracingSpi;
        }
        catch (IgniteSpiException e) {
            log.warning("Failed to start tracing processor with spi: " + getSpi().getName()
                + ". Noop implementation will be used instead.", e);

            throw e;
        }

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Adds tags with information about local node to given {@code span}.
     *
     * @param span Span.
     * @return Span enriched by local node information.
     */
    private Span enrichWithLocalNodeParameters(@Nullable Span span) {
        if (span == null)
            return null;

        span.addTag(SpanTags.NODE_ID, () -> ctx.localNodeId().toString());
        span.addTag(SpanTags.tag(NODE, SpanTags.NAME), ctx::igniteInstanceName);

        ClusterNode locNode = ctx.discovery().localNode();
        if (locNode != null && locNode.consistentId() != null)
            span.addTag(SpanTags.tag(NODE, SpanTags.CONSISTENT_ID), () -> locNode.consistentId().toString());

        return span;
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable Span parentSpan) {
        // Optimization for noop spi.
        if (noop)
            return NoopSpan.INSTANCE;

        // Optimization for zero sampling rate == 0.
        if ((parentSpan == NoopSpan.INSTANCE || parentSpan == null) &&
            tracingConfiguration.get(new TracingConfigurationCoordinates.Builder(spanType.scope()).build()).
                samplingRate() == SAMPLING_RATE_NEVER)
            return NoopSpan.INSTANCE;

        return enrichWithLocalNodeParameters(
            generateSpan(
                parentSpan,
                spanType,
                null));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable byte[] serializedParentSpan) {
        // Optimization for noop spi.
        if (noop)
            return NoopSpan.INSTANCE;

        // Optimization for zero sampling rate == 0.
        if ((serializedParentSpan.length == 0 || serializedParentSpan == null) &&
            tracingConfiguration.get(new TracingConfigurationCoordinates.Builder(spanType.scope()).build()).
                samplingRate() == SAMPLING_RATE_NEVER)
            return NoopSpan.INSTANCE;

        // 1 byte: special flags;
        // 1 bytes: spi type;
        // 2 bytes: major protocol version;
        // 2 bytes: minor protocol version;
        // 4 bytes: spi specific serialized span length;
        // n bytes: spi specific serialized span body;
        // 4 bytes: span type
        // 4 bytes included scopes size;
        // 2 * included scopes size: included scopes items one by one;

        Span span;

        try {
            if (serializedParentSpan == null || serializedParentSpan.length == 0)
                return create(spanType, NoopSpan.INSTANCE);

            // First byte of the serializedSpan is reserved for special flags - it's not used right now.

            // Deserialize and compare spi types. If they don't match (span was serialized with another spi) then
            // propagate serializedSpan as DeferredSpan.
            if (serializedParentSpan[SPI_TYPE_OFF] != getSpi().type())
                return new DeferredSpan(serializedParentSpan);

            // Deserialize and check major protocol version,
            // cause protocol might be incompatible in case of different protocol versions -
            // propagate serializedSpan as DeferredSpan.
            if (serializedParentSpan[MAJOR_PROTOCOL_VERSION_OFF] != MAJOR_PROTOCOL_VERSION)
                return new DeferredSpan(serializedParentSpan);

            // Deserialize and check minor protocol version.
            // within the scope of the same major protocol version, protocol should be backwards compatible
            byte minProtoVer = serializedParentSpan[MINOR_PROTOCOL_VERSION_OFF];

            // Deserialize spi specific span size.
            int spiSpecificSpanSize = bytesToInt(
                Arrays.copyOfRange(
                    serializedParentSpan,
                    SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH_OFF,
                    SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF),
                0);

            SpanType parentSpanType = null;

            Set<Scope> includedScopes = new HashSet<>();

            // Fall through.
            switch (minProtoVer) {
                case 0 : {
                    // Deserialize parent span type.
                    parentSpanType = SpanType.fromIndex(
                        bytesToInt(
                            Arrays.copyOfRange(
                                serializedParentSpan,
                                SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + spiSpecificSpanSize,
                                SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                                    spiSpecificSpanSize),
                            0));

                    // Deserialize included scopes size.
                    int includedScopesSize = bytesToInt(
                        Arrays.copyOfRange(
                            serializedParentSpan,
                            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                                spiSpecificSpanSize,
                            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                                INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSpanSize),
                        0);

                    // Deserialize included scopes one by one.
                    for (int i = 0; i < includedScopesSize; i++) {
                        includedScopes.add(Scope.fromIndex(
                            bytesToShort(
                                Arrays.copyOfRange(
                                    serializedParentSpan,
                                    SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                                        INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSpanSize +
                                        i * SCOPE_INDEX_BYTE_LENGTH,
                                    SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                                        INCLUDED_SCOPES_SIZE_BYTE_LENGTH + SCOPE_INDEX_BYTE_LENGTH +
                                        spiSpecificSpanSize + i * SCOPE_INDEX_BYTE_LENGTH),
                                0)));
                    }
                }
            }

            assert parentSpanType != null;

            // If there's is parent span and parent span supports given scope then...
            if (parentSpanType.scope() == spanType.scope() || includedScopes.contains(spanType.scope())) {
                // create new span as child span for parent span, using parents span included scopes.

                Set<Scope> mergedIncludedScopes = new HashSet<>(includedScopes);
                mergedIncludedScopes.add(parentSpanType.scope());
                mergedIncludedScopes.remove(spanType.scope());

                span = new SpanImpl(
                    getSpi().create(
                        spanType.spanName(),
                        Arrays.copyOfRange(
                            serializedParentSpan,
                            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF,
                            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + spiSpecificSpanSize)),
                    spanType,
                    mergedIncludedScopes);
            }
            else {
                // do nothing;
                return new DeferredSpan(serializedParentSpan);
                // "suppress" parent span for a while, create new span as separate one.
                // return spi.create(trace, null, includedScopes);
            }
        }
        catch (Exception e) {
            LT.warn(log, "Failed to create span from serialized value " +
                "[serializedValue=" + Arrays.toString(serializedParentSpan) + "]");

            span = NoopSpan.INSTANCE;
        }

        return enrichWithLocalNodeParameters(span);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Span create(
        @NotNull SpanType spanType,
        @Nullable Span parentSpan,
        @Nullable String lb
    ) {
        // Optimization for noop spi.
        if (noop)
            return NoopSpan.INSTANCE;

        // Optimization for zero sampling rate == 0.
        if ((parentSpan == NoopSpan.INSTANCE || parentSpan == null) &&
            tracingConfiguration.get(
                new TracingConfigurationCoordinates.Builder(spanType.scope()).withLabel(lb).build()).
                samplingRate() == SAMPLING_RATE_NEVER)
            return NoopSpan.INSTANCE;

        return enrichWithLocalNodeParameters(
            generateSpan(
                parentSpan,
                spanType,
                lb));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public byte[] serialize(@NotNull Span span) {
        // Optimization for noop spi.
        if (noop)
            return NoopTracing.NOOP_SERIALIZED_SPAN;

        // Optimization for NoopSpan.
        if (span == NoopSpan.INSTANCE)
            return NoopTracing.NOOP_SERIALIZED_SPAN;

        // 1 byte: special flags;
        // 1 bytes: spi type;
        // 2 bytes: major protocol version;
        // 2 bytes: minor protocol version;
        // 4 bytes: spi specific serialized span length;
        // n bytes: spi specific serialized span body;
        // 4 bytes: span type
        // 4 bytes included scopes size;
        // 2 * included scopes size: included scopes items one by one;

        if (span instanceof DeferredSpan)
            return ((DeferredSpan)span).serializedSpan();

        // Spi specific serialized span.
        byte[] spiSpecificSerializedSpan = getSpi().serialize(((SpanImpl)span).spiSpecificSpan());

        int serializedSpanLen = SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
            INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSerializedSpan.length + SCOPE_INDEX_BYTE_LENGTH *
            span.includedScopes().size();

        byte[] serializedSpanBytes = new byte[serializedSpanLen];

        // Skip special flags bytes.

        // Spi type idx.
        serializedSpanBytes[SPI_TYPE_OFF] = getSpi().type();

        // Major protocol version;
        serializedSpanBytes[MAJOR_PROTOCOL_VERSION_OFF] = MAJOR_PROTOCOL_VERSION;

        // Minor protocol version;
        serializedSpanBytes[MINOR_PROTOCOL_VERSION_OFF] = MINOR_PROTOCOL_VERSION;

        // Spi specific serialized span length.
        System.arraycopy(
            intToBytes(spiSpecificSerializedSpan.length),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH_OFF,
            SPI_SPECIFIC_SERIALIZED_SPAN_BYTES_LENGTH);

        // Spi specific span.
        System.arraycopy(
            spiSpecificSerializedSpan,
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF,
            spiSpecificSerializedSpan.length);

        // Span type.
        System.arraycopy(
            intToBytes(span.type().index()),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + spiSpecificSerializedSpan.length,
            PARENT_SPAN_TYPE_BYTES_LENGTH );

        assert span.includedScopes() != null;

        // Included scope size
        System.arraycopy(
            intToBytes(span.includedScopes().size()),
            0,
            serializedSpanBytes,
            SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                spiSpecificSerializedSpan.length,
            INCLUDED_SCOPES_SIZE_BYTE_LENGTH);

        int includedScopesCnt = 0;

        if (!span.includedScopes().isEmpty()) {
            for (Scope includedScope : span.includedScopes()) {
                System.arraycopy(
                    shortToBytes(includedScope.idx()),
                    0,
                    serializedSpanBytes,
                    SPI_SPECIFIC_SERIALIZED_SPAN_BODY_OFF + PARENT_SPAN_TYPE_BYTES_LENGTH +
                        INCLUDED_SCOPES_SIZE_BYTE_LENGTH + spiSpecificSerializedSpan.length +
                        SCOPE_INDEX_BYTE_LENGTH * includedScopesCnt++,
                    SCOPE_INDEX_BYTE_LENGTH);
            }
        }

        return serializedSpanBytes;
    }

    /**
     * Generates child span if it's possible due to parent/child included scopes, otherwise returns patent span as is.
     * @param parentSpan Parent span.
     * @param spanTypeToCreate Span type to create.
     * @param lb Label.
     */
    @SuppressWarnings("unchecked")
    private @NotNull Span generateSpan(
        @Nullable Span parentSpan,
        @NotNull SpanType spanTypeToCreate,
        @Nullable String lb
    ) {
        if (parentSpan instanceof DeferredSpan)
            return create(spanTypeToCreate, ((DeferredSpan)parentSpan).serializedSpan());

        if (parentSpan == NoopSpan.INSTANCE || parentSpan == null) {
            if (spanTypeToCreate.rootSpan()) {
                // Get tracing configuration.
                TracingConfigurationParameters tracingConfigurationParameters = tracingConfiguration.get(
                    new TracingConfigurationCoordinates.Builder(spanTypeToCreate.scope()).withLabel(lb).build());

                return shouldSample(tracingConfigurationParameters.samplingRate()) ?
                    new SpanImpl(
                        getSpi().create(
                            spanTypeToCreate.spanName(),
                            (SpiSpecificSpan)null),
                        spanTypeToCreate,
                        tracingConfigurationParameters.includedScopes()) :
                    NoopSpan.INSTANCE;
            }
            else
                return NoopSpan.INSTANCE;
        }
        else {
            // If there's is parent span and parent span supports given scope then...
            if (parentSpan.isChainable(spanTypeToCreate.scope())) {
                // create new span as child span for parent span, using parents span included scopes.

                Set<Scope> mergedIncludedScopes = new HashSet<>(parentSpan.includedScopes());

                mergedIncludedScopes.add(parentSpan.type().scope());
                mergedIncludedScopes.remove(spanTypeToCreate.scope());

                return new SpanImpl(
                    getSpi().create(
                        spanTypeToCreate.spanName(),
                        ((SpanImpl)parentSpan).spiSpecificSpan()),
                    spanTypeToCreate,
                    mergedIncludedScopes);
            }
            else {
                // do nothing;
                return NoopSpan.INSTANCE;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        // Optimization for noop spi.
        if (noop)
            return NOOP_TRACEABLE_MSG_HANDLER;

        return msgHnd;
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationManager configuration() {
        return tracingConfiguration;
    }

    /**
     * @param samlingRate Sampling rate.
     * @return {@code true} if according to given sampling-rate span should be sampled.
     */
    private boolean shouldSample(double samlingRate) {
        if (samlingRate == SAMPLING_RATE_NEVER)
            return false;

        return Math.random() <= samlingRate;
    }
}
