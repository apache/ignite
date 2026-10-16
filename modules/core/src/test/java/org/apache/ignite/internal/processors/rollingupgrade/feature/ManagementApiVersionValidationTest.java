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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestManagementVisorOneNodeTask;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/** */
public class ManagementApiVersionValidationTest extends AbstractRollingUpgradeManagementApiTest {
    /** */
    public static final LogListener DESERIALIZATION_FAILED_LSNR = LogListener.builder().andMatches(
        "Failed to deserialize the Ignite Management API command argument"
    ).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setGridLogger(new ListeningTestLogger(log, DESERIALIZATION_FAILED_LSNR));

        return cfg;
    }

    /** */
    @Test
    public void testCommandAcceptedOnlyFromCompatibleClientVersions() throws Exception {
        startGrid(0, "2.21.0");

        checkCommandArgumentDeserializationFailed("2.21.1");
        checkCommandArgumentDeserializationFailed("2.19.0");

        executeCommand("2.21.0");
        executeCommand("2.20.0");
    }

    /** */
    private void checkCommandArgumentDeserializationFailed(String cliVer) throws Exception {
        DESERIALIZATION_FAILED_LSNR.reset();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                executeCommand(cliVer);

                return null;
            },
            ClientConnectionException.class,
            "Channel is closed");

        DESERIALIZATION_FAILED_LSNR.check(getTestTimeout());
    }

    /** */
    private void executeCommand(String cliVer) throws Exception {
        executeCommandFromClient(0, 0, cliVer, TestManagementVisorOneNodeTask.class, "");
    }
}
