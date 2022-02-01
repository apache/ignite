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

package org.apache.ignite;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.DISCOVERY;
import static org.apache.ignite.spi.tracing.Scope.EXCHANGE;
import static org.apache.ignite.spi.tracing.Scope.TX;

/**
 * Tests for tracing configuration validation rules.
 */
@SuppressWarnings("ThrowableNotThrown")
public class TracingConfigurationValidationTest extends GridCommonAbstractTest {
    /**
     * Ensure that in case of null scope as part of tracing coordinates {@code IllegalArgumentException} is thrown with
     * message "Null scope is not valid for tracing coordinates."
     */
    @Test
    public void testThatItsNotPossibleToSetNullAsScopeForTracingCoordinates() {
        GridTestUtils.assertThrows(
            log,
            () -> new TracingConfigurationCoordinates.Builder(null).build(),
            IllegalArgumentException.class,
            "Null scope is not valid for tracing coordinates.");
    }

    /**
     * Ensure that it's possible to set any non-null {@link Scope} as part of TracingConfigurationCoordinates.
     */
    @Test
    public void testThatItsPossibleToSpecifyAnyScopeAsPartOfTracingConfigurationCoordinates() {
        for (Scope scope : Scope.values())
            new TracingConfigurationCoordinates.Builder(scope).build();
    }

    /**
     * Ensure that it's possible to set any string as label as part of TracingConfigurationCoordinates.
     */
    @Test
    public void testThatItsPossibleToSpecifyAnyStringAsLabelAsPartOfTracingConfigurationCoordinates() {
        Stream.of(
            null,
            "",
            "label", "Some really long label with spaces, Some really long label with spaces," +
                " Some really long label with spaces, Some really long label with spaces," +
                " Some really long label with spaces").forEach(
            lb -> new TracingConfigurationCoordinates.Builder(TX).withLabel(lb).build());
    }

    /**
     * Ensure that it's possible to set any 0<= double <= 1 as sampling rate as part of TracingConfigurationParameters.
     */
    @Test
    public void testThatItsPossibleToSpecifyAnyDoubleBetweenZeroAndOneIncludingAsSamplingRate() {
        for (double validSamplingRate : new double[] {
            0,
            0.1,
            0.11,
            0.1234567890,
            1})
            new TracingConfigurationParameters.Builder().withSamplingRate(validSamplingRate).build();
    }

    /**
     * Ensure that in case of invalid sampling rate {@code IllegalArgumentException} is thrown with message "Specified
     * sampling rate=[invalidVal] has invalid value.Should be between 0 and 1 including boundaries."
     */
    @Test
    public void testThatItsPossibleToSpecifyOnlyValidSamplingRate() {
        for (Double invalidSamplingRate : new double[] {
            -1d,
            10d})
        {
            GridTestUtils.assertThrows(
                log,
                () -> new TracingConfigurationParameters.Builder().withSamplingRate(invalidSamplingRate).build(),
                IllegalArgumentException.class,
                "Specified sampling rate=[" + invalidSamplingRate +
                    "] has invalid value. Should be between 0 and 1 including boundaries.");
        }
    }

    /**
     * Ensure that it's possible to set any set of {@code Scope} as included scopes as part of
     * {@link TracingConfigurationParameters}.
     */
    @Test @SuppressWarnings("unchecked")
    public void testThatItsPossibleToSpecifyAnySetincludedScopesAsPartOfTracingConfigurationParameters() {
        for (Set<Scope> validincludedScopes : new Set[] {
            null,
            Collections.emptySet(),
            Collections.singleton(COMMUNICATION),
            new HashSet<>(Arrays.asList(COMMUNICATION, DISCOVERY, TX, EXCHANGE))})
            new TracingConfigurationParameters.Builder().withIncludedScopes(validincludedScopes).build();
    }
}
