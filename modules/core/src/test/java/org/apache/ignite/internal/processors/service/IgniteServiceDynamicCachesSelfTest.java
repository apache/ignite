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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteServiceDynamicCachesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployCalledAfterCacheStart() throws Exception {
        String cacheName = "cache";

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);
        ccfg.setBackups(1);

        Ignite ig = ignite(0);

        ig.createCache(ccfg);

        try {
            final IgniteServices svcs = ig.services();

            final String svcName = "myService";

            svcs.deployKeyAffinitySingleton(svcName, new TestService(), cacheName, primaryKey(ig.cache(cacheName)));

            boolean res = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return svcs.service(svcName) != null;
                }
            }, 10 * 1000);

            assertTrue("Service was not deployed", res);

            ig.destroyCache(cacheName);

            res = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return svcs.service(svcName) == null;
                }
            }, 10 * 1000);

            assertTrue("Service was not undeployed", res);
        }
        finally {
            ig.services().cancelAll();

            ig.destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDeployCalledBeforeCacheStart() throws Exception {
        String cacheName = "cache";

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);
        ccfg.setBackups(1);

        IgniteEx ig = grid(0);

        final IgniteServices svcs = ig.services();

        final String svcName = "myService";

        ig.createCache(ccfg);

        Object key = primaryKey(ig.cache(cacheName));

        ig.destroyCache(cacheName);

        awaitPartitionMapExchange();

        if (ig.context().service() instanceof GridServiceProcessor) {
            svcs.deployKeyAffinitySingleton(svcName, new TestService(), cacheName, key);

            assertNull(svcs.service(svcName));

            ig.createCache(ccfg);
        }
        else if (ig.context().service() instanceof IgniteServiceProcessor) {
            GridTestUtils.assertThrowsWithCause(() -> {
                svcs.deployKeyAffinitySingleton(svcName, new TestService(), cacheName, key);

                return null;
            }, ServiceDeploymentException.class);

            ig.createCache(ccfg);

            svcs.deployKeyAffinitySingleton(svcName, new TestService(), cacheName, key);
        }
        else
            fail("Unexpected service implementation.");

        try {
            boolean res = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return svcs.service(svcName) != null;
                }
            }, 10 * 1000);

            assertTrue("Service was not deployed", res);

            info("stopping cache: " + cacheName);

            ig.destroyCache(cacheName);

            res = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return svcs.service(svcName) == null;
                }
            }, 10 * 1000);

            assertTrue("Service was not undeployed", res);
        }
        finally {
            ig.services().cancelAll();

            ig.destroyCache(cacheName);
        }
    }

    /**
     *
     */
    private static class TestService implements Service {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            log.info("Service cancelled.");
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            log.info("Service deployed.");
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            log.info("Service executed.");
        }
    }
}
