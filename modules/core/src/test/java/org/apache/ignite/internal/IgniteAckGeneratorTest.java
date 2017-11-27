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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_START_ON_CLIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SUCCESS_FILE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteAckGeneratorTest extends GridCommonAbstractTest {
    /** Logger. */
    private IgniteLogger log = Mockito.spy(new GridStringLogger());

    /** Config. */
    private static IgniteConfiguration cfg;

    /** User attributes. */
    private Map<String, String> userAttr = new HashMap<>();

    /** Logger string. */
    private static String logStr;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        when(log.isInfoEnabled()).thenReturn(true);

        userAttr.put("Name", "John Gold");

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setUserAttributes(userAttr);

        cfg.setMemoryConfiguration(new MemoryConfiguration());

        cfg.setGridLogger(log);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        logStr = log.toString();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     *
     */
    public void testAckAsciiLogo() throws Exception {
        assertTrue(logStr.contains(ACK_VER_STR));
    }

    /**
     *
     */
    public void testAckConfigUrl() throws Exception {
        assertTrue(logStr.contains(System.getProperty(IGNITE_CONFIG_URL, "n/a")));
    }

    /**
     *
     */
    public void testAckDaemon() throws Exception {
        String demon = "Daemon mode: " + (cfg.isDaemon() ? "on" : "off");

        assertTrue(logStr.contains(demon));
    }

    /**
     *
     */
    public void testAckOsInfo() throws Exception {
        assertTrue(logStr.contains(U.osString()));

        assertTrue(logStr.contains(System.getProperty("user.name")));

        assertTrue(logStr.contains(String.valueOf(U.jvmPid())));
    }

    /**
     *
     */
    public void testAckLanguageRuntime() throws Exception {
        assertTrue(logStr.contains(U.jdkString()));

        assertTrue(logStr.contains(String.valueOf(U.heapSize(2))));

        assertTrue(logStr.contains(String.valueOf(U.jdkName())));
    }

    /**
     *
     */
    public void testAckRemoteManagement() throws Exception {
        boolean isClientNode = (cfg.isClientMode() != null && cfg.isClientMode()) || cfg.isDaemon();

        boolean isJmxRemoteEnabled = System.getProperty("com.sun.management.jmxremote") != null;

        boolean isRestartEnabled = System.getProperty(IGNITE_SUCCESS_FILE) != null;

        boolean isRestEnabled = cfg.getConnectorConfiguration() != null && (!isClientNode || IgniteSystemProperties.getBoolean(IGNITE_REST_START_ON_CLIENT));

        assertTrue(logStr.contains("restart: " + onOff(isRestartEnabled)));

        assertTrue(logStr.contains("REST: " + onOff(isRestEnabled)));

        assertTrue(logStr.contains("remote: " + onOff(isJmxRemoteEnabled)));
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    private String onOff(boolean b) {
        return b ? "on" : "off";
    }

    /**
     *
     */
    public void testAckVmArguments() throws Exception {
        RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();

        assertTrue(logStr.contains(cfg.getIgniteHome()));

        assertTrue(logStr.contains(String.valueOf(rtBean.getInputArguments())));
    }

    /**
     *
     */
    public void testAckMemoryConfiguration() throws Exception {
        MemoryConfiguration memCfg = cfg.getMemoryConfiguration();

        assert
            memCfg != null;

        assertTrue(logStr.contains(String.valueOf(memCfg.getSystemCacheInitialSize() / (1024 * 1024))));
    }

    /**
     *
     */
    public void testAckCacheConfiguration() throws Exception {
        CacheConfiguration[] cacheCfgs = cfg.getCacheConfiguration();

        for (CacheConfiguration c : cacheCfgs)
            assertTrue(logStr.contains(c.getName()));
    }

    /**
     *
     */
    public void testAckP2pConfiguration() throws Exception {
        String p2pConfiguration = "Peer class loading is enabled (disable it in production for performance and " +
            "deployment consistency reasons)";

        assertTrue(logStr.contains(p2pConfiguration));
    }

    /**
     *
     */
    public void testLogNodeUserAttributes() throws Exception {
        for (Map.Entry<?, ?> attr : cfg.getUserAttributes().entrySet()) {
            assertTrue(logStr.contains(String.valueOf(attr.getKey())));

            assertTrue(logStr.contains(String.valueOf(attr.getKey())));
        }
    }

    /**
     *
     */
    public void testAckStart() throws Exception {
        assertTrue(logStr.contains(VER_STR));

        assertTrue(logStr.contains(BUILD_TSTAMP_STR));

        assertTrue(logStr.contains(REV_HASH_STR));
    }
}
