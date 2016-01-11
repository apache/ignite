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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class IgniteUpdateNotifierPerClusterSettingSelfTest extends GridCommonAbstractTest {
    /** */
    private String backup;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backup = System.getProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, backup);

        stopAllGrids();

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return threadCount("ignite-update-notifier-timer") == 0;
            }
        }, 5 * 60000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotifierEnabledForCluster() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(true));

        startGrid(0);

        assertEquals(1, threadCount("ignite-update-notifier-timer"));

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(false));

        startGrid(1);

        assertEquals(2, threadCount("ignite-update-notifier-timer"));

        // Failover.
        stopGrid(0); // Kill oldest.

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return threadCount("ignite-update-notifier-timer") == 1;
            }
        }, 5 * 60000);

        startGrid(2);

        assertEquals(2, threadCount("ignite-update-notifier-timer"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotifierDisabledForCluster() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(false));

        startGrid(0);

        assertEquals(0, threadCount("ignite-update-notifier-timer"));

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, String.valueOf(true));

        startGrid(1);

        assertEquals(0, threadCount("ignite-update-notifier-timer"));

        // Failover.
        stopGrid(0); // Kill oldest.

        startGrid(2);

        assertEquals(0, threadCount("ignite-update-notifier-timer"));
    }

    /**
     * @param name Thread name.
     * @return Count of thread with the name.
     */
    private int threadCount(String name) {
        int cnt = 0;

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (name.equals(t.getName()))
                cnt++;
        }

        return cnt;
    }
}
