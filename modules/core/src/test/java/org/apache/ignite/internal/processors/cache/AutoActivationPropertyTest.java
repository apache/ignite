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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 * Checks that {@link IgniteConfiguration#autoActivation} works correctly after deprecation and introduction {@link
 * IgniteConfiguration#clusterStateOnStart}.
 */
public class AutoActivationPropertyTest extends GridCommonAbstractTest {
    /** Auto activation flag. */
    private boolean autoActivation;

    /** */
    private Map<String, LogListener> logListeners = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        LogListener lsnr = LogListener.matches(
            "Property autoActivation deprecated. Use clusterStateOnStart instead."
        ).build();

        testLog.registerListener(lsnr);

        logListeners.put(igniteInstanceName, lsnr);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog)
            .setAutoActivationEnabled(autoActivation)
            .setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testAutoActivationEnabled() throws Exception {
        autoActivation = true;

        checkProperty();
    }

    /** */
    @Test
    public void testAutoActivationDisabled() throws Exception {
        autoActivation = false;

        checkProperty();
    }

    /** */
    private void checkProperty() throws Exception {
        final int nodeCnt = 2;

        IgniteEx crd = startGrids(nodeCnt);

        for (int i = 0; i < nodeCnt; i++) {
            assertEquals(autoActivation, grid(i).configuration().isAutoActivationEnabled());

            assertNull(grid(i).configuration().getClusterStateOnStart());
        }

        crd.cluster().state(ACTIVE);

        stopAllGrids();

        crd = startGrids(nodeCnt);

        for (int i = 0; i < nodeCnt; i++)
            assertEquals(autoActivation ? ACTIVE : INACTIVE, crd.cluster().state());

        for (String name : logListeners.keySet())
            assertTrue(name, logListeners.get(name).check());
    }
}
