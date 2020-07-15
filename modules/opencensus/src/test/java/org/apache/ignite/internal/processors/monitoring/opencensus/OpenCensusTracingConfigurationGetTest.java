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

import java.util.Collections;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.TX;

/**
 * Tests for OpenCensus based {@link TracingConfigurationManager#get(TracingConfigurationCoordinates)}.
 */
public class OpenCensusTracingConfigurationGetTest extends AbstractTracingTest {
    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /**
     * Ensure that label specific configuration get returns default scope specific if there's
     * neither corresponding custom label specific nor corresponding custom scope specific.
     */
    @Test
    public void testThatLabelSpecificConfigurationGetReturnsDefaultIfCustomConfigurationNotSet() {
        assertEquals(
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().get(
                new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build()));
    }

    /**
     * Ensure that label specific configuration get returns custom scope specific if there's no
     * corresponding custom label specific one.
     */
    @Test
    public void testThatLabelSpecificConfigurationGetReturnsCustomScopeSpecificIfLabelSpecificIsNotSet() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.2).
                withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().set(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().get(
                new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build()));
    }

    /**
     * Ensure that label specific configuration get returns custom label specific is such one is present.
     */
    @Test
    public void testThatLabelSpecificConfigurationGetReturnsLabelSpecificOne() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.35).
                withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().set(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().get(coords));
    }

    /**
     * Ensure that scope specific configuration get returns default scope specific if there's no corresponding
     * custom specific one.
     */
    @Test
    public void testThatScopeSpecificConfigurationGetReturnsDefaultOneIfCustomConfigurationNotSet() {
        assertEquals(
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().get(
                new TracingConfigurationCoordinates.Builder(TX).build()));
    }

    /**
     * Ensure that scope specific configuration get returns corresponding custom specific one if it's available.
     */
    @Test
    public void testThatScopeSpecificConfigurationGetReturnsCustomScopeSpecific() {
        TracingConfigurationCoordinates coords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.2).
                withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().set(coords, expScopeSpecificParameters);

        assertEquals(
            expScopeSpecificParameters,
            grid(0).tracingConfiguration().get(coords));
    }

    /**
     * Ensure that scope specific configuration get returns corresponding custom specific one if it's available
     * and ignores label specific one.
     */
    @Test
    public void testThatScopeSpecificConfigurationGetReturnsScopeSpecificEventIfLabelSpecificIsSet() {
        TracingConfigurationCoordinates scopeSpecificCoords =
            new TracingConfigurationCoordinates.Builder(TX).build();

        TracingConfigurationCoordinates lbSpecificCoords =
            new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

        TracingConfigurationParameters expScopeSpecificParameters =
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.35).
                withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

        grid(0).tracingConfiguration().set(lbSpecificCoords, expScopeSpecificParameters);

        assertEquals(
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().get(scopeSpecificCoords));
    }
}
