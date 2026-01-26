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

package org.apache.ignite.compatibility.spi.discovery;

import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/** */
public class TcpDiscoveryDifferentClusterVersionsTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final String SER_MODE_MSG = "serMode byte is expected";

    /** */
    private static final IgniteReleasedVersion OLD_VERSION = IgniteReleasedVersion.VER_2_17_0;

    /** */
    private LogListener serModeListener;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        setLoggerDebugLevel();

        serModeListener = LogListener.matches(SER_MODE_MSG).build();

        ListeningTestLogger listeningLog = new ListeningTestLogger(log);

        listeningLog.registerListener(serModeListener);

        return super.getConfiguration(igniteInstanceName).setGridLogger(listeningLog);
    }

    /**
     * Compatibility test that ensures previous-version client fails to connect to current server
     * and server reports missing serMode byte.
     */
    @Test
    public void testOldClientRejected() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrows(
            log,
            () -> startGrid("old-client", OLD_VERSION.toString(), null),
            AssertionError.class,
            null
        );

        assertTrue("Expected serMode error in server log.", serModeListener.check(getTestTimeout()));
    }
}
