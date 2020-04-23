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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID;

/**
 * Checks consistent id in various cases.
 */
public class ConsistentIdImplicitlyExplicitlyTest extends GridCommonAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** Ipaddress with port pattern. */
    private static final String IPADDRESS_WITH_PORT_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
        "([01]?\\d\\d?|2[0-4]\\d|25[0-5]):[1-9][0-9]+$";

    /** Pattern. */
    private static final Pattern ADDR_WITH_PORT_PATTERN = Pattern.compile(IPADDRESS_WITH_PORT_PATTERN);

    /** Uuid as string pattern. */
    private static final String UUID_STR_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}" +
        "-[a-fA-F0-9]{12}";

    /** Pattern. */
    private static final Pattern UUID_PATTERN = Pattern.compile(UUID_STR_PATTERN);

    /** */
    private static final String WARN_MESSAGE = "Consistent ID is not set, it is recommended to set consistent ID for " +
        "production clusters (use IgniteConfiguration.setConsistentId property)";

    /** Configured consistent id. */
    private Serializable defConsistentId;

    /** Persistence enabled. */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setConsistentId(defConsistentId)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)
                    .setMaxSize(200L * 1024 * 1024)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        defConsistentId = null;
        persistenceEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        log.clearListeners();

        stopAllGrids();

        defConsistentId = null;

        if (persistenceEnabled) {
            cleanPersistenceDir();

            persistenceEnabled = false;
        }
    }

    /**
     * Consistent ID is not configured neither in the {@link IgniteConfiguration} nor via the JVM property.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testConsistentIdNotConfigured() throws Exception {
        LogListener lsnr = expectLogEvent(WARN_MESSAGE, 0);

        Ignite ignite = startGrid(0);

        assertNull(ignite.configuration().getConsistentId());

        assertTrue(ADDR_WITH_PORT_PATTERN.matcher(ignite.cluster().localNode().consistentId().toString()).find());

        assertTrue(lsnr.check());

        info("Consistent ID: " + ignite.cluster().localNode().consistentId());
    }

    /**
     * Consistent ID is not configurent neither in the {@link IgniteConfiguration} nor in the JVM property, and
     * persistent enabled.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testConsistentIdNotConfiguredWithPersistence() throws Exception {
        persistenceEnabled = true;

        LogListener lsnr = expectLogEvent(WARN_MESSAGE, 1);

        Ignite ignite = startGrid(0);

        assertNull(ignite.configuration().getConsistentId());

        assertTrue(UUID_PATTERN.matcher(ignite.cluster().localNode().consistentId().toString()).find());

        assertTrue(lsnr.check());

        info("Consistent ID: " + ignite.cluster().localNode().consistentId());
    }

    /**
     * Consistent ID is not configured in the {@link IgniteConfiguration}, but set in the JVM property.
     *
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_OVERRIDE_CONSISTENT_ID, value = "JvmProp consistent id")
    public void testConsistentIdConfiguredInJvmProp() throws Exception {
        String specificConsistentId = System.getProperty(IGNITE_OVERRIDE_CONSISTENT_ID);

        LogListener lsnr = expectLogEvent(WARN_MESSAGE, 0);

        Ignite ignite = startGrid(0);

        assertEquals(specificConsistentId, ignite.configuration().getConsistentId());

        assertEquals(specificConsistentId, ignite.cluster().localNode().consistentId());

        assertTrue(lsnr.check());

        info("Consistent ID: " + ignite.cluster().localNode().consistentId());
    }

    /**
     * Consistent ID is configured in the {@link IgniteConfiguration}, but not in the JVM property.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testConsistentIdConfiguredInIgniteCfg() throws Exception {
        defConsistentId = "IgniteCfg consistent id";

        LogListener lsnr = expectLogEvent(WARN_MESSAGE, 0);

        Ignite ignite = startGrid(0);

        assertEquals(defConsistentId, ignite.configuration().getConsistentId());

        assertEquals(defConsistentId, ignite.cluster().localNode().consistentId());

        assertTrue(lsnr.check());

        info("Consistent ID: " + ignite.cluster().localNode().consistentId());
    }

    /**
     * Consistent ID is configured in the {@link IgniteConfiguration} and in the JVM property.
     *
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_OVERRIDE_CONSISTENT_ID, value = "JvmProp consistent id")
    public void testConsistentIdConfiguredInIgniteCfgAndJvmProp() throws Exception {
        defConsistentId = "IgniteCfg consistent id";

        String specificConsistentId = System.getProperty(IGNITE_OVERRIDE_CONSISTENT_ID);;

        LogListener lsnr = expectLogEvent(WARN_MESSAGE, 0);

        Ignite ignite = startGrid(0);

        assertEquals(specificConsistentId, ignite.configuration().getConsistentId());

        assertEquals(specificConsistentId, ignite.cluster().localNode().consistentId());

        assertTrue(lsnr.check());

        info("Consistent ID: " + ignite.cluster().localNode().consistentId());
    }

    /**
     * @param eventMsg Event message.
     * @param cnt Number of expected events.
     */
    private LogListener expectLogEvent(String eventMsg, int cnt) {
        LogListener lsnr = LogListener.matches(s -> s.startsWith(eventMsg)).times(cnt).build();

        log.registerListener(lsnr);

        return lsnr;
    }
}
