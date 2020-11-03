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

package org.apache.ignite.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.TracingConfigurationCommand;
import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationTaskResult;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.EXCHANGE;
import static org.apache.ignite.spi.tracing.Scope.TX;

/**
 * Tests for {@link TracingConfigurationCommand}
 */
public class GridCommandHandlerTracingConfigurationTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Default configuration map. */
    private static final Map<TracingConfigurationCoordinates, TracingConfigurationParameters> DFLT_CONFIG_MAP =
        new HashMap<>();

    /** TX scope specific coordinates to be used within several tests. */
    private static final TracingConfigurationCoordinates TX_SCOPE_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(TX).build();

    /** EXCHANGE scope specific coordinates to be used within several tests. */
    private static final TracingConfigurationCoordinates EXCHANGE_SCOPE_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(EXCHANGE).build();

    /** Updated scope specific parameters to be used within several tests. */
    private static final TracingConfigurationParameters SOME_SCOPE_SPECIFIC_PARAMETERS =
        new TracingConfigurationParameters.Builder().withSamplingRate(0.75).
            withIncludedScopes(Collections.singleton(COMMUNICATION)).build();

    /** TX Label specific coordinates to be used within several tests. */
    private static final TracingConfigurationCoordinates TX_LABEL_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(TX).withLabel("label").build();

    /** Updated label specific parameters to be used within several tests. */
    private static final TracingConfigurationParameters SOME_LABEL_SPECIFIC_PARAMETERS =
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

    /** */
    protected IgniteEx ignite;

    /** */
    private static CommandHandler hnd;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(2);

        hnd = new CommandHandler();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Cleanup configuration.
        grid(0).tracingConfiguration().resetAll(null);

        // Populate tracing with some custom configurations.
        grid(0).tracingConfiguration().set(
            TX_SCOPE_SPECIFIC_COORDINATES,
            SOME_SCOPE_SPECIFIC_PARAMETERS);

        grid(0).tracingConfiguration().set(
            TX_LABEL_SPECIFIC_COORDINATES,
            SOME_LABEL_SPECIFIC_PARAMETERS);

        grid(0).tracingConfiguration().set(
            EXCHANGE_SCOPE_SPECIFIC_COORDINATES,
            SOME_SCOPE_SPECIFIC_PARAMETERS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Do nothing;
    }

    /**
     * Ensure that in case of "--tracing-configuration" without arguments
     * tracing configuration for all scopes will be returned.
     */
    @Test
    public void testTracingConfigurationWithoutSubCommandsReturnsTracingConfiguratoinForAllScopes() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration"));

        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> expTracingCfg =
            new HashMap<>(DFLT_CONFIG_MAP);

        expTracingCfg.put(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);
        expTracingCfg.put(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);
        expTracingCfg.put(EXCHANGE_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expTracingCfg.forEach(expRes::add);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration get_all --scope TX"
     * TX based tracing configuration will be returned: both scope specific and label specific.
     */
    @Test
    public void testGetAllWithScopeReturnsOnlySpecifiedScopeSpecificConfiguratoin() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "get_all", "--scope", "TX"));

        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);
        expRes.add(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration get_all" without scope
     * tracing configuration for all scopes will be returned.
     */
    @Test
    public void testGetAllWithoutScopeReturnsTracingConfigurationsForAllScopes() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "get_all"));

        // Check command result.
        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> expTracingCfg =
            new HashMap<>(DFLT_CONFIG_MAP);

        expTracingCfg.put(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);
        expTracingCfg.put(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);
        expTracingCfg.put(EXCHANGE_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expTracingCfg.forEach(expRes::add);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration get --scope TX"
     * TX specific and only TX specific tracing configuration will be returned:
     * TX-label specific configuration not expected.
     */
    @Test
    public void testGetWithScopeReturnsScopeSpecificConfiguratoin() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "get", "--scope", "TX"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration get --scope TX --label label"
     * TX label specific and only TX label specific tracing configuration will be returned:
     * TX specific configuration not expected.
     */
    @Test
    public void testGetWithScopeAndLabelReturnsLabelSpecificConfigurationIfSuchOneExists() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "get", "--scope",
            "TX", "--label", "label"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration reset_all --scope TX"
     * TX based configuration will be reseted and returned:
     */
    @Test
    public void testResetAllWithScopeResetsScopeBasedConfigurationAndReturnsIt() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "reset_all", "--scope", "TX"));

        // Ensure that configuration was actually reseted.
        assertEquals(
            Collections.singletonMap(
                TX_SCOPE_SPECIFIC_COORDINATES,
                TracingConfigurationManager.DEFAULT_TX_CONFIGURATION),
            grid(0).tracingConfiguration().getAll(TX));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_SCOPE_SPECIFIC_COORDINATES, TracingConfigurationManager.DEFAULT_TX_CONFIGURATION);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration reset_all"
     * Whole tracing configurations will be reseted and returned.
     */
    @Test
    public void testResetAllWithoutScopeResetsTracingConfigurationForAllScopesAndReturnsIt() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "reset_all"));

        // Ensure that configuration was actually reseted.
        assertEquals(
            Collections.singletonMap(
                TX_SCOPE_SPECIFIC_COORDINATES,
                TracingConfigurationManager.DEFAULT_TX_CONFIGURATION),
            grid(0).tracingConfiguration().getAll(TX));

        assertEquals(
            Collections.singletonMap(
                EXCHANGE_SCOPE_SPECIFIC_COORDINATES,
                TracingConfigurationManager.DEFAULT_EXCHANGE_CONFIGURATION),
            grid(0).tracingConfiguration().getAll(EXCHANGE));

        // Check command result.
        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> expTracingCfg =
            new HashMap<>(DFLT_CONFIG_MAP);

        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expTracingCfg.forEach(expRes::add);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration reset --scope TX"
     * TX scope specific configuration will be reseted, TX label specific configuration should stay unchanged.
     * Whole TX based configuration should be returned.
     */
    @Test
    public void testResetWithScopeResetsScopeSpecificConfiguratoinAndReturnesScopeBasedConfiguration() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "reset", "--scope", "TX"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_SCOPE_SPECIFIC_COORDINATES, TracingConfigurationManager.DEFAULT_EXCHANGE_CONFIGURATION);
        expRes.add(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration reset --scope TX --label label"
     * TX label specific configuration will be removed, TX scope specific configuration should stay unchanged.
     * Whole TX based configuration should be returned.
     */
    @Test
    public void testResetWithScopeAndLabelResetsLabelSpecificConfiguratoinAndReturnesScopeBasedConfiguration() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "reset", "--scope", "TX",
            "--label", "label"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(TX_SCOPE_SPECIFIC_COORDINATES, SOME_SCOPE_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration set --scope TX --sampling-rate 0.123
     * --included-scopes COMMUNICATION,EXCHANGE"
     * TX scope specific configuration should be updated.
     * Whole TX based configuration should be returned.
     */
    @Test
    public void testSetWithScopeSetsScopeSpecificConfiguratoinAndReturnesScopeBasedConfiguration() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "set", "--scope", "TX",
            "--sampling-rate", "0.123", "--included-scopes", "COMMUNICATION,EXCHANGE"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(
            TX_SCOPE_SPECIFIC_COORDINATES,
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.123).
                withIncludedScopes(new HashSet<>(Arrays.asList(COMMUNICATION, EXCHANGE))).build());

        expRes.add(TX_LABEL_SPECIFIC_COORDINATES, SOME_LABEL_SPECIFIC_PARAMETERS);

        verifyResult(expRes);
    }

    /**
     * Ensure that in case of "--tracing-configuration set --scope TX --label label --sampling-rate 0.123
     * --included-scopes COMMUNICATION,EXCHANGE"
     * TX label specific configuration should be updated.
     * Whole TX based configuration should be returned.
     */
    @Test
    public void testSetWithScopeAndLabelSetsLabelSpecificConfiguratoinAndReturnsScopeBasedConfiguration() {
        assertEquals(EXIT_CODE_OK, execute(hnd, "--tracing-configuration", "set", "--scope", "TX",
            "--label", "label", "--sampling-rate", "0.123", "--included-scopes", "COMMUNICATION,EXCHANGE"));

        // Check command result.
        VisorTracingConfigurationTaskResult expRes = new VisorTracingConfigurationTaskResult();

        expRes.add(
            TX_SCOPE_SPECIFIC_COORDINATES,
            SOME_SCOPE_SPECIFIC_PARAMETERS);

        expRes.add(
            TX_LABEL_SPECIFIC_COORDINATES,
            new TracingConfigurationParameters.Builder().
                withSamplingRate(0.123).
                withIncludedScopes(new HashSet<>(Arrays.asList(COMMUNICATION, EXCHANGE))).build());

        verifyResult(expRes);
    }

    /**
     * Verify that expected result equals got one.
     *
     * @param expRes Expected command result.
     */
    private void verifyResult(VisorTracingConfigurationTaskResult expRes) {
        VisorTracingConfigurationTaskResult gotRes = hnd.getLastOperationResult();

        assertNotNull(gotRes);

        assertNotNull(gotRes.tracingConfigurations());

        assertTrue(expRes.tracingConfigurations().containsAll(gotRes.tracingConfigurations()) &&
            gotRes.tracingConfigurations().containsAll(expRes.tracingConfigurations()));
    }
}
