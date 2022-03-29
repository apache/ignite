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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteExtendedLogoTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger testLog;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setGridLogger(testLog);
    }

    /** */
    @After
    public void afterLogo() {
        testLog.clearListeners();

        stopAllGrids();
    }

    /** @throws Exception If fails. */
    @Test
    public void testExtendedLogo() throws Exception {
        testLog = new ListeningTestLogger(log);

        LogListener waitLogoLsnr = LogListener.matches("Ignite InfoProvider is used to customize the logo version output.")
            .build();
        testLog.registerListener(waitLogoLsnr);

        startGrid();

        assertTrue(waitForCondition(waitLogoLsnr::check, 5_000L));
    }
}
