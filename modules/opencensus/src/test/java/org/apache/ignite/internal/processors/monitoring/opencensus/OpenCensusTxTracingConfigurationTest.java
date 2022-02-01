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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.TX;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for transaction tracing configuration.
 */
public class OpenCensusTxTracingConfigurationTest extends AbstractTracingTest {
    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /**
     * Ensure that in case of sampling rate equals to 0.0 (Never) no transactions are traced.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testTxConfigurationSamplingRateNeverPreventsTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_NEVER).build());

        client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        Set<String> unexpectedTxSpanNames = Arrays.stream(SpanType.values()).
            filter(spanType -> spanType.scope() == TX).
            map(SpanType::spanName).
            collect(Collectors.toSet());

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> unexpectedTxSpanNames.contains(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }

    /**
     * Ensure that in case of sampling rate equals to 1.0 (Always) transactions are successfully traced.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testTxConfigurationSamplingRateAlwaysEnablesTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());
    }

    /**
     * Ensure that specifying 0 < sapling rate < 1 within TX scope will trace some but not all transactions.
     * Cause of probability nature of sampling, it's not possible to check that 0.5 sampling rate
     * will result in exactly half of the transactions being traced.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testTxConfigurationSamplingRateHalfSamplesSomethingAboutHalfTransactions() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(0.5).build());

        final int txAmount = 100;

        for (int i = 0; i < txAmount; i++)
            client.transactions().txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        // Cause of probability nature of sampling, it's not possible to check that 0.5 sampling rate will end with
        // 5 sampling transactions out of {@code txAmount},
        // so we just check that some and not all transactions were traced.
        assertTrue(!gotSpans.isEmpty() && gotSpans.size() < txAmount);
    }


    /**
     * Ensure that TX traces doesn't include COMMUNICATION sub-traces in case of empty set of included scopes.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testTxTraceDoesNotIncludeCommunicationTracesInCaseOfEmptyIncludedScopes() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        SpanId parentSpanId = handler().allSpans()
            .filter(span -> SpanType.TX_NEAR_PREPARE.spanName().equals(span.getName()))
            .collect(Collectors.toList()).get(0).getContext().getSpanId();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> parentSpanId.equals(span.getParentSpanId()) &&
                SpanType.COMMUNICATION_SOCKET_WRITE.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }

    /**
     * Ensure that TX trace does include COMMUNICATION sub-traces in case of COMMUNICATION scope within the set
     * of included scopes of the corresponding TX tracing configuration.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testTxTraceIncludesCommunicationTracesInCaseOfCommunicationScopeInTxIncludedScopes() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).
                withIncludedScopes(Collections.singleton(Scope.COMMUNICATION)).
                build());

        Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        tx.commit();

        handler().flush();

        SpanId parentSpanId = handler().allSpans()
            .filter(span -> SpanType.TX_NEAR_PREPARE.spanName().equals(span.getName()))
            .collect(Collectors.toList()).get(0).getContext().getSpanId();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> parentSpanId.equals(span.getParentSpanId()) &&
                SpanType.COMMUNICATION_SOCKET_WRITE.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertFalse(gotSpans.isEmpty());
    }

    /**
     * Ensure that label specific configuration is used instead of scope specific if it's possible.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testThatLabelSpecificConfigurationIsUsedWheneverPossible() throws Exception {
        IgniteEx client = startGrid("client");

        final String txLbToBeTraced = "label1";

        final String txLbNotToBeTraced = "label2";

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).withLabel(txLbToBeTraced).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().withLabel(txLbToBeTraced).txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());

        // Not to be traced, cause there's neither tracing configuration with given label
        // nor scope specific tx configuration. In that case default tx tracing configuration will be used that
        // actually disables tracing.
        client.transactions().withLabel(txLbNotToBeTraced).txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        // Still only one, previously detected, span is expected.
        assertEquals(1, gotSpans.size());
    }

    /**
     * Ensure that scope specific configuration is used if corresponding label specific not found.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testThatScopeSpecificConfigurationIsUsedIfLabelSpecificNotFound() throws Exception {
        IgniteEx client = startGrid("client");

        client.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(TX).build(),
            new TracingConfigurationParameters.Builder().withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(1, gotSpans.size());
    }

    /**
     * Ensure that default scope specific configuration is used if there's no neither label specif not custom scope specific ones.
     * Also ensure that by default TX tracing is disabled.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testThatDefaultConfigurationIsUsedIfScopeSpecificNotFoundAndThatByDefaultTxTracingIsDisabled()
        throws Exception {
        IgniteEx client = startGrid("client");

        client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE).commit();

        handler().flush();

        java.util.List<SpanData> gotSpans = handler().allSpans()
            .filter(span -> SpanType.TX.spanName().equals(span.getName()))
            .collect(Collectors.toList());

        assertTrue(gotSpans.isEmpty());
    }
}
