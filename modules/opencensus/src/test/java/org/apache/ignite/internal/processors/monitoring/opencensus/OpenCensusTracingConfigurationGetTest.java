/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.Collections;

import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationManager;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationCoordinates;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.processors.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.internal.processors.tracing.Scope.TX;

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

    /**
     * Ensure that default configuration returns
     * in case of calling {@code tracingConfiguration().get()} if distributed metastorage is not available.
     */
    @Test
    @SystemPropertiesList({
        @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "false"),
        @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "false"),
        @WithSystemProperty(key = IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE, value = "false")
    })
    public void testThatDefaultTracingConfigurationIsUsedIfMetastorageIsDisabled() {
        assertEquals(
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION,
            grid(0).tracingConfiguration().get(TX_SCOPE_SPECIFIC_COORDINATES)
        );
    }
}
