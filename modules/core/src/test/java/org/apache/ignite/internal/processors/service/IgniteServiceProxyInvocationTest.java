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
import org.apache.ignite.IgniteServices;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Test service proxy invocation guaranties.
 */
public class IgniteServiceProxyInvocationTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_SERVICE_NAME = "TestService";

    /** */
    @Before
    public void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /**
     * Getting proxy on a deployment-initiator node should not fail even it's called immediately after deployment
     * finished.
     * <p/>
     * Ignite guarantees once deployment finished successfully - a service or a service proxy must be available from
     * the service deployment-initiator node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void callProxyAfterDeploymentOnInitiator() throws Exception {
        try {
            Ignite initiator = startGrid(0);
            startGrid(1);

            initiator.services(initiator.cluster().forRemotes())
                .deployClusterSingleton(TEST_SERVICE_NAME, new TestServiceImpl());

            TestService proxy = initiator.services()
                .serviceProxy(TEST_SERVICE_NAME, TestService.class, false);

            Assert.assertEquals(42, proxy.getAnswer());

            Assert.assertNull(initiator.services().service(TEST_SERVICE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Getting proxy on a NON-deployment-initiator node might fail right after deployment finished on deployment
     * initiator node.
     * <p/>
     * It takes some time to provide service deployment result to the whole cluster, equal discovery-spi round trip time
     * in general.
     * <p/>
     * Ignite provides some kind of "eventual consistency" guaranties in this case, that means successfully deployed
     * service eventually becomes available for invocation from NON-deployment-initiator nodes.
     * <p/>
     * Use API with a timeout to get a proxy: {@link IgniteServices#serviceProxy(String, Class, boolean, long)} that
     * allows waiting for deployment result then get the proxy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void callProxyAfterDeploymentOnRemote() throws Exception {
        try {
            Ignite initiator = startGrid(0);
            Ignite remoteNode = startGrid(1);

            initiator.services(initiator.cluster().forLocal())
                .deployClusterSingletonAsync(TEST_SERVICE_NAME, new TestServiceImpl());

            TestService proxy = remoteNode.services()
                .serviceProxy(TEST_SERVICE_NAME, TestService.class, false, 2_000L);

            Assert.assertEquals(42, proxy.getAnswer());

            Assert.assertNotNull(initiator.services().service(TEST_SERVICE_NAME));

            Assert.assertNull(remoteNode.services().service(TEST_SERVICE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private interface TestService extends Service {
        /** */
        public int getAnswer();
    }

    /** */
    private static class TestServiceImpl implements TestService {
        /** {@inheritDoc} */
        @Override public int getAnswer() {
            return 42;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {

        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {

        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {

        }
    }
}
