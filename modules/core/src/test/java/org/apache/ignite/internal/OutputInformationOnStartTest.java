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

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.mockito.Mockito.when;

/**
 *
 */
public class OutputInformationOnStartTest extends GridCommonAbstractTest {
    /**
     * Logger.
     */
    IgniteLogger log = Mockito.spy(new GridStringLogger());

    /**
     * Config.
     */
    IgniteConfiguration cfg;

    /** */
    private OutputVariousInformation outputVariousInformation = new OutputVariousInformation(log, cfg);

    /**
     * User attributes.
     */
    private Map<String, String> userattr = new HashMap<>();

    /** Default configuration. */
    private String dfltConfiguration = "default";

    /** Debug configuration. */
    private String debugConfiguration = "debug";

    /** Info enabled configuration. */
    private String infoEnabledConfiguration = "infoEnabled";

    /** User attributes configuration. */
    private String userAttrsConfiguration = "userAttributes";

    /** Memory configuration. */
    private String memoryConfiguration = "memoryConfiguration";

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals("userAttributes")) {
            when(log.isInfoEnabled()).thenReturn(true);

            userattr.put("Name", "John Gold");

            cfg.setPeerClassLoadingEnabled(true);

            cfg.setUserAttributes(userattr);
        }

        if (igniteInstanceName.equals("debug"))
            when(log.isDebugEnabled()).thenReturn(true);

        if (igniteInstanceName.equals("infoEnabled"))
            when(log.isInfoEnabled()).thenReturn(true);

        if (igniteInstanceName.equals("memoryConfiguration"))
            cfg.setMemoryConfiguration(new MemoryConfiguration());

        cfg.setGridLogger(log);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    /**
     *
     */
    public void testAckAsciiLogo() throws Exception {
        startGrid(dfltConfiguration);

        String asciiLogoPart = ">>> Ignite documentation: " + "http://";

        assertTrue(log.toString().contains(asciiLogoPart));
    }

    /**
     *
     */
    public void testAckConfigUrl() throws Exception {
        startGrid(dfltConfiguration);

        String cfgUrl = "Config URL: " + System.getProperty(IGNITE_CONFIG_URL, "n/a");

        assertTrue(log.toString().contains(cfgUrl));
    }

    /**
     *
     */
    public void testAckDaemon() throws Exception {
        startGrid(dfltConfiguration);

        String demon = "Daemon mode: " + (cfg.isDaemon() ? "on" : "off");

        assertTrue(log.toString().contains(demon));
    }

    /**
     *
     */
    public void testAckOsInfo() throws Exception {
        startGrid(dfltConfiguration);

        String osIfnoPart = "OS: " + U.osString();

        assertTrue(log.toString().contains(osIfnoPart));
    }

    /**
     *
     */
    public void testAckLanguageRuntime() throws Exception {
        startGrid(dfltConfiguration);

        String languageRuntimePart = "VM information: " + U.jdkString();

        assertTrue(log.toString().contains(languageRuntimePart));
    }

    /**
     *
     */
    public void testAckRemoteManagement() throws Exception {
        startGrid(dfltConfiguration);

        String remoteManagementPart = "Remote Management [";

        assertTrue(log.toString().contains(remoteManagementPart));
    }

    /**
     *
     */
    public void testAckVmArguments() throws Exception {
        startGrid(dfltConfiguration);

        String vmArgumentsPart = "IGNITE_HOME=" + cfg.getIgniteHome();

        assertTrue(log.toString().contains(vmArgumentsPart));
    }

    /**
     *
     */
    public void testAckClassPaths() throws Exception {
        startGrid(debugConfiguration);

        log.debug("go");

        String clsPathsPart = "Boot class path: "/* + ManagementFactory.getRuntimeMXBean()*/;

        System.out.println(log.isDebugEnabled());

        log.toString();

//        System.out.println(log.toString());

//        assertTrue(log.toString().contains(clsPathsPart));
    }

    /**
     *
     */
    public void testAckSystemProperties() throws Exception {
        startGrid(debugConfiguration);

        String sysPropsPart = "System property [";

        assertTrue(log.toString().contains(sysPropsPart));
    }

    /**
     *
     */
    public void testAckEnvironmentVariables() throws Exception {
        startGrid(debugConfiguration);

        String enviromentVariablesPart = "Environment variable [";

        assertTrue(log.toString().contains(enviromentVariablesPart));
    }

    /**
     *
     */
    public void testAckMemoryConfiguration() throws Exception {
        startGrid(memoryConfiguration);

        String memoryConfigurationPart = "System cache's MemoryPolicy size is configured to ";

        assertTrue(log.toString().contains(memoryConfigurationPart));
    }

    /**
     *
     */
    public void testAckCacheConfiguration() throws Exception {
        startGrid(dfltConfiguration);

        String cacheConfigurationPart = "Configured caches [";

        assertTrue(log.toString().contains(cacheConfigurationPart));
    }

    /**
     *
     */
    public void testAckP2pConfiguration() {
        String p2pConfiguration = "Peer class loading is enabled (disable it in production for performance and " +
                "deployment consistency reasons)";

        outputVariousInformation.ackP2pConfiguration();

        assertTrue(log.toString().contains(p2pConfiguration));
    }

    /**
     *
     */
    public void testLogNodeUserAttributes() throws Exception {
        startGrid(userAttrsConfiguration);

        String logNodeUserAttribute = "Local node user attribute [Name=John Gold]";

        assertTrue(log.toString().contains(logNodeUserAttribute));
    }

    /**
     *
     */
    public void testAckSpis() throws Exception {
        startGrid(debugConfiguration);

        String spisPart = "START SPI LIST:";

        System.out.println(log.toString());

//        assertTrue(log.toString().contains(spisPart));
    }

    /**
     *
     */
    public void testAckSecurity() throws Exception {
        startGrid(infoEnabledConfiguration);

        String securityPath = "Security status [authentication=";

        assertTrue(log.toString().contains(securityPath));
    }

    /**
     *
     */
    public void testAckStart() throws Exception {
        startGrid(infoEnabledConfiguration);

        String startPath = "Ignite ver. ";

        assertTrue(log.toString().contains(startPath));
    }
}
