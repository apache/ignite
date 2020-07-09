/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_ACTIVE_ON_START;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_AUTO_ACTIVATION;

/**
 * Checks that {@link IgniteConfiguration#getClusterStateOnStart()} works correctly separately and together with
 * {@link IgniteConfiguration#isActiveOnStart()} and {@link IgniteConfiguration#isAutoActivationEnabled()}.
 */
public class ClusterStateOnStartPropertyTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private Map<String, LogListener> logListeners = new HashMap<>();

    /** */
    private ClusterState state;

    /** Persistence enabled flag. */
    private boolean persistence;

    /** Auto activation flag. Null means that property should be skipped. */
    private Boolean autoActivation;

    /** Active on state flag. Null means that property should be skipped. */
    private Boolean activeOnStart;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setClusterStateOnStart(state)
            .setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME))
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(persistence)
                    )
            );

        if (autoActivation != null)
            cfg.setAutoActivationEnabled(autoActivation);

        if (activeOnStart != null)
            cfg.setActiveOnStart(activeOnStart);

        // Warn messages must be printed only if both options (old and new) are presented.
        if (autoActivation != null || activeOnStart != null) {
            ListeningTestLogger testLog = new ListeningTestLogger(false, log);

            LogListener lsnr = LogListener.matches(
                persistence ?
                    "Property `autoActivation` will be ignored due to the property `clusterStateOnStart` is presented." :
                    "Property `activeOnStart` will be ignored due to the property `clusterStateOnStart` is presented."
            ).build();

            testLog.registerListener(lsnr);

            logListeners.put(igniteInstanceName, lsnr);

            cfg.setGridLogger(testLog);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    // Simple scenarios with in-memory cluster.

    /**
     * @see #checkPropertyInMemory(ClusterState).
     */
    @Test
    public void testInactiveInMemory() throws Exception {
        checkPropertyInMemory(INACTIVE);
    }

    /**
     * @see #checkPropertyInMemory(ClusterState).
     */
    @Test
    public void testActiveInMemory() throws Exception {
        checkPropertyInMemory(ACTIVE);
    }

    /**
     * @see #checkPropertyInMemory(ClusterState).
     */
    @Test
    public void testReadOnlyInMemory() throws Exception {
        checkPropertyInMemory(ACTIVE_READ_ONLY);
    }

    // Simple scenarios with persistent cluster.

    /**
     * @see #checkPropertyPersistence(ClusterState).
     */
    @Test
    public void testInactivePersistence() throws Exception {
        checkPropertyPersistence(INACTIVE);
    }

    /**
     * @see #checkPropertyPersistence(ClusterState).
     */
    @Test
    public void testActivePersistence() throws Exception {
        checkPropertyPersistence(ACTIVE);
    }

    /**
     * @see #checkPropertyPersistence(ClusterState).
     */
    @Test
    public void testReadOnlyPersistence() throws Exception {
        checkPropertyPersistence(ACTIVE_READ_ONLY);
    }

    // Scenarios for both (activeOnStart, clusterStateOnStart) properties with in-memory cluster.

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testInactiveInactiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(INACTIVE, false);
    }

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testInactiveActiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(INACTIVE, true);
    }

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testActiveInactiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(ACTIVE, false);
    }

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testActiveActiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(ACTIVE, true);
    }

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testReadOnlyInactiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(ACTIVE_READ_ONLY, false);
    }

    /**
     * @see #checkBothPropertiesInMemory(ClusterState, boolean).
     */
    @Test
    public void testReadOnlyActiveOnStartInMemory() throws Exception {
        checkBothPropertiesInMemory(ACTIVE_READ_ONLY, true);
    }

    // Scenarios for both (autoActivation, clusterStateOnStart) properties with persistent cluster.

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testInactiveDisableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(INACTIVE, false);
    }

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testInactiveEnableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(INACTIVE, true);
    }

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testActiveDisableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(ACTIVE, false);
    }

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testActiveEnableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(ACTIVE, true);
    }

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testReadOnlyDisableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(ACTIVE_READ_ONLY, false);
    }

    /**
     * @see #checkBothPropertiesPersistent(ClusterState, boolean).
     */
    @Test
    public void testReadOnlyEnableAutoActivation() throws Exception {
        checkBothPropertiesPersistent(ACTIVE_READ_ONLY, true);
    }

    /**
     * Checks that in memory cluster works correctly with given {@code state} value of
     * {@link IgniteConfiguration#clusterStateOnStart} property and {@code activeOnStart} property together. Only
     * {@code state} property must be applied.
     *
     * @param state Given value of {@link IgniteConfiguration#clusterStateOnStart} property.
     * @param activeOnStart Given value of {@link IgniteConfiguration#activeOnStart} property.
     * @throws Exception If failed.
     */
    private void checkBothPropertiesInMemory(ClusterState state, boolean activeOnStart) throws Exception {
        this.state = state;
        this.activeOnStart = activeOnStart;

        startGrids(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++) {
            checkNodeConfig(grid(i).configuration(), false, state, null, activeOnStart);

            assertEquals(state, grid(i).cluster().state());
        }

        for (String name : logListeners.keySet())
            assertTrue(name, logListeners.get(name).check());
    }

    /**
     * Checks that persistent cluster works correctly with given {@code state} value of
     * {@link IgniteConfiguration#clusterStateOnStart} property and {@code autoActivation} property together. Only
     * {@code state} property must be applied.
     *
     * @param state Given value of {@link IgniteConfiguration#clusterStateOnStart} property.
     * @param autoActivation Given value of {@link IgniteConfiguration#autoActivation} property.
     * @throws Exception If failed.
     */
    private void checkBothPropertiesPersistent(ClusterState state, boolean autoActivation) throws Exception {
        persistence = true;

        this.state = state;
        this.autoActivation = autoActivation;

        IgniteEx crd = startGrids(NODES_CNT);

        crd.cluster().state(ACTIVE);

        for (int i = 0; i < NODES_CNT; i++) {
            checkNodeConfig(grid(i).configuration(), true, state, autoActivation, null);

            assertEquals(ACTIVE, grid(i).cluster().state());
        }

        for (String name : logListeners.keySet())
            assertTrue(name, logListeners.get(name).check());

        stopAllGrids();

        startGrids(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++)
            assertEquals(state, grid(i).cluster().state());
    }

    /**
     * Checks that in memory cluster works correctly with given {@code state} value of
     * {@link IgniteConfiguration#clusterStateOnStart} property.
     *
     * @param state Given value.
     * @throws Exception If failed.
     */
    private void checkPropertyInMemory(ClusterState state) throws Exception {
        this.state = state;

        startGrids(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++) {
            checkNodeConfig(grid(i).configuration(), false, state, null, null);

            assertEquals(state, grid(i).cluster().state());
        }
    }

    /**
     * Checks that persistent cluster works correctly with given {@code state} value of
     * {@link IgniteConfiguration#clusterStateOnStart} property.
     *
     * @param state Given value.
     * @throws Exception If failed.
     */
    private void checkPropertyPersistence(ClusterState state) throws Exception {
        persistence = true;

        this.state = state;

        IgniteEx crd = startGrids(NODES_CNT);

        crd.cluster().state(ACTIVE);

        for (int i = 0; i < NODES_CNT; i++) {
            checkNodeConfig(grid(i).configuration(), true, state, null, null);

            assertEquals(ACTIVE, grid(i).cluster().state());
        }

        stopAllGrids();

        startGrids(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++)
            assertEquals(state, grid(i).cluster().state());
    }

    /** */
    private static void checkNodeConfig(
        IgniteConfiguration cfg,
        boolean persistenceEnabled,
        ClusterState state,
        @Nullable Boolean autoActivation,
        @Nullable Boolean activeOnStart
    ) {
        assertEquals(persistenceEnabled, CU.isPersistenceEnabled(cfg));
        assertEquals(state, cfg.getClusterStateOnStart());
        assertEquals(autoActivation == null ? DFLT_AUTO_ACTIVATION : autoActivation, cfg.isAutoActivationEnabled());
        assertEquals(activeOnStart == null ? DFLT_ACTIVE_ON_START : activeOnStart, cfg.isActiveOnStart());
    }
}
