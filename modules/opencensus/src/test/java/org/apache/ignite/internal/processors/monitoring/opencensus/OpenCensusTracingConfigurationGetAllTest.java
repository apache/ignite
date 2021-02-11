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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.TX;

/**
 * Tests for OpenCensus based {@link TracingConfigurationManager#getAll(Scope)}.
 */
public class OpenCensusTracingConfigurationGetAllTest extends AbstractTracingTest {
    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /**
     * Ensure that getAll() retrieves default transaction configuration.
     */
    @Test
    public void testThatDefaultConfigurationReturnsIfScopeNotSpecifiedAndCustomConfigurationNotSet() {
        assertEquals(
            DFLT_CONFIG_MAP,
            grid(0).tracingConfiguration().getAll(null));
    }

    /**
     * Ensure that getAll(Scope) retrieves default scope specific transaction configuration.
     */
    @Test
    public void testThatDefaultScopeSpecificConfigurationReturnsIfScopeIsSpecifiedAndCustomConfigurationNotSet() {
        assertEquals(
            Collections.singletonMap(
                TX_SCOPE_SPECIFIC_COORDINATES,
                TracingConfigurationManager.DEFAULT_TX_CONFIGURATION),
            grid(0).tracingConfiguration().getAll(TX));
    }

    /**
     * Update any scope specific configuration and add some label specific one.
     * Ensure that getAll() retrieves tracing configuration including updated one.
     */
    @Test
    public void testThatCustomConfigurationReturnsIfScopeNotSpecifiedAndCustomConfigurationIsSet() {
        grid(0).tracingConfiguration().set(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        grid(0).tracingConfiguration().set(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> expTracingCfg =
            new HashMap<>(DFLT_CONFIG_MAP);

        expTracingCfg.put(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        expTracingCfg.put(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        assertEquals(
            expTracingCfg,
            grid(0).tracingConfiguration().getAll(null));
    }

    /**
     * Update any scope specific configuration and add some label specific one.
     * Ensure that getAll(scope) retrieves updated scope specific configuration.
     */
    @Test
    public void testThatCustomScopeSpecificConfigurationReturnsIfScopeIsSpecifiedAndCustomConfigurationIsSet() {
        grid(0).tracingConfiguration().set(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        grid(0).tracingConfiguration().set(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> expTracingCfg = new HashMap<>();

        expTracingCfg.put(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        expTracingCfg.put(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        assertEquals(
            expTracingCfg,
            grid(0).tracingConfiguration().getAll(TX));
    }
}
