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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.client.thin.TestTask;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;

/** */
public class GridDeploymentLocalStoreReuseTest extends AbstractThinClientTest {
    /** */
    private static final String LOG_NAME = "org.apache.ignite";

    /** */
    private static final int EXEC_CNT = 3;

    /** */
    private static Level initLogLevel;

    /** */
    private static ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1000)))
            .setGridLogger(testLog)
            .setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        LoggerConfig logCfg = LoggerContext.getContext(false).getConfiguration().getLoggerConfig(LOG_NAME);

        initLogLevel = logCfg.getLevel();

        Configurator.setLevel(LOG_NAME, Level.TRACE);

        testLog = new ListeningTestLogger(log);

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        Configurator.setLevel(LOG_NAME, initLogLevel);

        initLogLevel = null;

        super.afterTest();
    }

    /**
     * Verifies that multiple task executions do not cause excessive local deployment cache misses. The "deployment not
     * found ... clsLdrId=null" message is allowed only once upon initial task execution. The trace-level "deployment
     * was found for class with the local app class loader" messages must be present in the output marking the subsequent
     * task executions without cache misses.
     */
    @Test
    public void testNoExcessiveLocalDeploymentCacheMisses() throws Exception {
        String taskClsName = TestTask.class.getName();

        String notFoundMsg = String.format(
            "Deployment was not found for class with specific class loader [alias=%s, clsLdrId=null]", taskClsName);

        String foundLocLdrMsg = String.format(
            "Deployment was found for class with the local app class loader [alias=%s]", taskClsName);

        LogListener lsnr0 = LogListener.matches(notFoundMsg).times(1).build();
        LogListener lsnr1 = LogListener.matches(foundLocLdrMsg).atLeast(EXEC_CNT - 1).build();

        testLog.registerListener(lsnr0);
        testLog.registerListener(lsnr1);

        try (IgniteClient client = startClient(0)) {
            for (int i = 0; i < EXEC_CNT; i++)
                client.compute().execute(TestTask.class.getName(), null);
        }

        assertTrue(lsnr0.check(5_000));
        assertTrue(lsnr1.check(5_000));
    }
}
