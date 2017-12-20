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

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests service proxy timeouts.
 */
public class IgniteServiceProxyTimeoutInitializedTest extends GridCommonAbstractTest {
    /** */
    private static Service srvc;

    /** */
    private static CountDownLatch latch1;

    /** */
    private static CountDownLatch latch2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        final ServiceConfiguration scfg = new ServiceConfiguration();

        if (gridName.endsWith("0")) {
            scfg.setName("testService");
            scfg.setService(srvc);
            scfg.setMaxPerNodeCount(1);
            scfg.setTotalCount(1);
            scfg.setNodeFilter(new NodeFilter());

            final Map<String, String> attrs = new HashMap<>();

            attrs.put("clusterGroup", "0");

            cfg.setUserAttributes(attrs);

            cfg.setServiceConfiguration(scfg);
        }

        cfg.setMarshaller(null);

        final BinaryConfiguration binCfg = new BinaryConfiguration();

        // Despite defaults explicitly set to lower case.
        binCfg.setIdMapper(new BinaryBasicIdMapper(true));

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks that we limit retries to get not available service by timeout.
     *
     * @throws Exception If fail.
     */
    @SuppressWarnings({"Convert2Lambda", "ThrowableResultOfMethodCallIgnored"})
    public void testUnavailableService() throws Exception {
        srvc = new TestWaitServiceImpl();

        latch1 = new CountDownLatch(1);
        latch2 = new CountDownLatch(1);

        try {
            GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(0);

                    return null;
                }
            });

            assert latch1.await(1, TimeUnit.MINUTES);

            final IgniteEx ignite1 = startGrid(1);

            final TestService testSrvc = ignite1.services().serviceProxy("testService", TestService.class, false, 500);

            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    testSrvc.test();

                    return null;
                }
            }, IgniteException.class, null);
        }
        finally {
            latch2.countDown();
        }
    }

    /**
     * Checks that service not hangs if timeout set. Here we get hang with marshalling exception.
     *
     * @throws Exception If fail.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "Convert2Lambda"})
    public void testServiceException() throws Exception {
        srvc = new HangServiceImpl();

        // Start service grid.
        startGrid(0);
        final IgniteEx ignite1 = startGrid(1);

        final HangService testSrvc = ignite1.services().serviceProxy("testService", HangService.class, false, 1_000);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                testSrvc.hang();

                return null;
            }
        }, ComputeTaskTimeoutException.class, null);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     *
     */
    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(final ClusterNode clusterNode) {
            return "0".equals(clusterNode.attribute("clusterGroup"));
        }
    }

    /**
     *
     */
    private interface TestService {
        /** */
        void test();
    }

    /**
     *
     */
    private static class TestWaitServiceImpl implements Service, TestService {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void test() {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void cancel(final ServiceContext ctx) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void init(final ServiceContext ctx) throws Exception {
            latch1.countDown();

            // Simulate long initialization.
            latch2.await(1, TimeUnit.MINUTES);
        }

        /** {@inheritDoc} */
        @Override public void execute(final ServiceContext ctx) throws Exception {
            // No-op
        }
    }

    /**
     *
     */
    private static class HangClass implements Binarylizable {

        /** {@inheritDoc} */
        @Override public void writeBinary(final BinaryWriter writer) throws BinaryObjectException {
            try {
                U.sleep(10_000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new BinaryObjectException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void readBinary(final BinaryReader reader) throws BinaryObjectException {
            try {
                U.sleep(10_000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new BinaryObjectException(e);
            }
        }
    }

    /**
     *
     */
    private interface HangService {
        /**
         * @return Hangs deserialization.
         */
        HangClass hang();
    }

    /**
     *
     */
    private static class HangServiceImpl implements HangService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public HangClass hang() {
            return new HangClass();
        }

        /** {@inheritDoc} */
        @Override public void cancel(final ServiceContext ctx) {

        }

        /** {@inheritDoc} */
        @Override public void init(final ServiceContext ctx) throws Exception {

        }

        /** {@inheritDoc} */
        @Override public void execute(final ServiceContext ctx) throws Exception {

        }
    }
}
