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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.EXPRITY_POLICY_MSG;

/**
 * Test for checking logging of expiry policy info per cache.
 */
public class ExpiryPolicyInfoLoggingTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_1_NAME = "cache1";

    /** */
    private static final String CACHE_2_NAME = "cache2";

    /** */
    private static final String STARTED_CACHE_MSG = "Started cache [name=%s";

    /** */
    private static final String STARTED_CACHE_IN_RECOVERY_MODE_MSG = "Started cache in recovery mode [name=%s,";

    /** */
    private boolean persistenceEnabled;

    /** */
    private final ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

    /** */
    @Override public IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName)
            .setGridLogger(log)
            .setCacheConfiguration(
                new CacheConfiguration(CACHE_1_NAME)
                    .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(new Duration(TimeUnit.DAYS, 2)))
                    .setEagerTtl(true)
            ).setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled)
                )
            );
    }

    /**
     * Checking logging of expiry policy info for statically created cache.
     */
    @Test
    public void checkLoggingExpiryInfoForStaticallyCreatedCache() throws Exception {
        LogListener lsnr = LogListener
            .matches(s -> s.startsWith(String.format(STARTED_CACHE_MSG, CACHE_1_NAME)) &&
                s.contains(String.format(EXPRITY_POLICY_MSG, ModifiedExpiryPolicy.class.getName(), true))
            ).times(1)
            .build();

        log.registerListener(lsnr);

        startGrid(0);

        assertTrue(lsnr.check());
    }

    /**
     * Checking logging of expiry policy info for dynamically created cache.
     */
    @Test
    public void checkLoggingExpiryInfoForDynamicallyCreatedCache() throws Exception {
        LogListener lsnr = LogListener
            .matches(s -> s.startsWith(String.format(STARTED_CACHE_MSG, CACHE_2_NAME)) &&
                s.contains(String.format(EXPRITY_POLICY_MSG, AccessedExpiryPolicy.class.getName(), false))
            ).times(1)
            .build();

        log.registerListener(lsnr);

        IgniteEx srv = startGrid(0);

        srv.createCache(
            new CacheConfiguration<>(CACHE_2_NAME)
                .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 5)))
                .setEagerTtl(false)
        );

        assertTrue(lsnr.check());
    }

    /**
     * Checking logging of expiry policy info for statically created cache wich started in recovery mode.
     */
    @Test
    public void checkLoggingExpiryInfoForStaticallyCreatedCacheStartedInRecoveryMode() throws Exception {
        persistenceEnabled = true;

        LogListener lsnr = LogListener
            .matches(s -> s.startsWith(String.format(STARTED_CACHE_IN_RECOVERY_MODE_MSG, CACHE_1_NAME)) &&
                s.contains(String.format(EXPRITY_POLICY_MSG, ModifiedExpiryPolicy.class.getName(), true))
            ).times(1)
            .build();

        log.registerListener(lsnr);

        startGrid(0);

        assertTrue(lsnr.check());
    }

    /** */
    @Before
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @After
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
