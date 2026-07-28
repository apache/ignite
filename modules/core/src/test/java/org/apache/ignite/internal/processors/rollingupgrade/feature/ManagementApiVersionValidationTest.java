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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestManagementVisorOneNodeTask;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/** */
public class ManagementApiVersionValidationTest extends AbstractRollingUpgradeTest {
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
    public void testVersionValidation() throws Exception {
        withCoreVersion("2.21.0", () -> {
            startGrid(0);

            checkCommandArgumentDeserializationFailed(0, "2.21.1");
            checkCommandArgumentDeserializationFailed(0, "2.19.0");

            executeCommand(createCommandArgument(0, "2.21.0"));
            executeCommand(createCommandArgument(0, "2.20.0"));

            return null;
        });
    }

    /** */
    private void checkCommandArgumentDeserializationFailed(int destNodeIdx, String ver) throws Exception {
        DESERIALIZATION_FAILED_LSNR.reset();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                executeCommand(createCommandArgument(destNodeIdx, ver));

                return null;
            },
            ClientConnectionException.class,
            "Channel is closed");

        DESERIALIZATION_FAILED_LSNR.check(getTestTimeout());
    }

    /** */
    private void executeCommand(VisorTaskArgument<Object> arg) throws Exception {
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            cli.compute().execute(TestManagementVisorOneNodeTask.class.getName(), arg);
        }
    }

    /** */
    private VisorTaskArgument<Object> createCommandArgument(int destNodeIdx, String ver) throws Exception {
        return withCoreVersion(ver, () -> new VisorTaskArgument<>(nodeId(destNodeIdx), "", false));
    }

    /** */
    private static <R> R withCoreVersion(String ver, Callable<R> action) throws Exception {
        IgniteCoreFeatureSet prev = IgniteCoreFeatureSet.INSTANCE;
        IgniteCoreFeatureSet.INSTANCE = createCoreFeatureSet(ver);

        try {
            return action.call();
        }
        finally {
            IgniteCoreFeatureSet.INSTANCE = prev;
        }
    }
}
