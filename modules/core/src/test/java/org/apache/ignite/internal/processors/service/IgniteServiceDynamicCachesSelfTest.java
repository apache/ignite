/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
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
    public void testDeployCalledBeforeCacheStart() throws Exception {
        String cacheName = "cache";

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);
        ccfg.setBackups(1);

        Ignite ig = ignite(0);

        final IgniteServices svcs = ig.services();

        final String svcName = "myService";

        ig.createCache(ccfg);

        Object key = primaryKey(ig.cache(cacheName));

        ig.destroyCache(cacheName);

        awaitPartitionMapExchange();

        svcs.deployKeyAffinitySingleton(svcName, new TestService(), cacheName, key);

        assert svcs.service(svcName) == null;

        ig.createCache(ccfg);

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
