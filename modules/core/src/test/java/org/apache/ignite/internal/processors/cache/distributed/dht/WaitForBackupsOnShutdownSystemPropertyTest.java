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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;

/**
 * Check a log message if {@link IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN} is used.
 */
public class WaitForBackupsOnShutdownSystemPropertyTest extends GridCommonAbstractTest {
    /** Listening test logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        listeningLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /**
     * Check the message is printed if IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN is used.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "false")
    public void testWaitForBackupsOnShutdownPropertyExists() throws Exception {
        LogListener lnsr = LogListener.matches("IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN system property " +
                "is deprecated and will be removed in a future version. Use ShutdownPolicy instead.")
            .build();

        listeningLog.registerListener(lnsr);

        startGrid();

        assertTrue("The message was not found", lnsr.check());
    }

    /**
     * Check the message is not printed if IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN is not used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWaitForBackupsOnShutdownPropertyNotExists() throws Exception {
        LogListener lnsr = LogListener.matches("IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN system property " +
                "is deprecated and will be removed in a future version. Use ShutdownPolicy instead.")
            .build();

        listeningLog.registerListener(lnsr);

        startGrid();

        assertFalse("The message was found", lnsr.check());
    }
}
