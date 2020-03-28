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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Full API services test.
 */
public class IgniteServiceConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Test service name. */
    private static final String SERVICE_NAME = "testService";

    /** Timeout to wait finish of a service's deployment. */
    private static final long DEPLOYMENT_WAIT_TIMEOUT = 10_000L;

    /** Test service name. */
    private static final String CACHE_NAME = "testCache";

    /** */
    protected static final int CLIENT_NODE_IDX_2 = 4;

    /** Test object id counter. */
    private static int cntr;

    /** Callable factories. */
    private static final Factory[] serviceFactories = new Factory[] {
        Parameters.factory(TestServiceImpl.class),
        Parameters.factory(TestServiceImplExternalizable.class),
    };

    /** */
    private static boolean isEventDrivenServiceProcessorEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        isEventDrivenServiceProcessorEnabled = grid(0).context().service() instanceof IgniteServiceProcessor;
    }

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        int i = testsCfg.gridCount();

        if (i < 5)
            return super.expectedClient(testGridName);

        // Use two client nodes if grid index 5 or greater.
        return getTestIgniteInstanceName(CLIENT_NODE_IDX).equals(testGridName)
            || getTestIgniteInstanceName(CLIENT_NODE_IDX_2).equals(testGridName);
    }

    /**
     * Test node singleton deployment
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingletonDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(true, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) throws Exception {
                services.deployNodeSingleton(svcName, (Service)svc);

                waitForServiceDeploymentIfNeeded(services, svcName);
            }
        }));
    }

    /**
     * Test cluster singleton deployment
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterSingletonDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) throws Exception {
                services.deployClusterSingleton(svcName, (Service)svc);

                waitForServiceDeploymentIfNeeded(services, svcName);
            }
        }));
    }

    /**
     * Test key affinity deployment
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11883")
    @Test
    public void testKeyAffinityDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                IgniteCache<Object, Object> cache = grid(testedNodeIdx).getOrCreateCache(CACHE_NAME);

                try {
                    services.deployKeyAffinitySingleton(svcName, (Service)svc, cache.getName(), primaryKey(cache));
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }));
    }

    /**
     * Tests multiple deployment
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(true, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) throws Exception {
                services.deployMultiple(svcName, (Service)svc, 0, 1);

                waitForServiceDeploymentIfNeeded(services, svcName);
            }
        }));
    }

    /**
     * Tests deployment.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) throws Exception {
                services.deployClusterSingleton(svcName, (Service)svc);

                ServiceConfiguration cfg = new ServiceConfiguration();

                cfg.setName(svcName);

                cfg.setService((Service)svc);

                cfg.setTotalCount(1);

                cfg.setMaxPerNodeCount(1);

                cfg.setNodeFilter(services.clusterGroup().predicate());

                services.deploy(cfg);

                waitForServiceDeploymentIfNeeded(services, svcName);
            }
        }));
    }

    /**
     * Service test closure
     */
    private class ServiceTestRunnable implements TestRunnable {
        /** Sticky. */
        private final boolean sticky;

        /** Deploy closure */
        private final DeployClosure deployC;

        /**
         * Default constructor.
         *
         * @param sticky Sticky flag.
         * @param deployC Closure.
         */
        public ServiceTestRunnable(boolean sticky, DeployClosure deployC) {
            this.sticky = sticky;

            this.deployC = deployC;
        }

        /** {@inheritDoc} */
        @Override public void run() throws Exception {
            for (Factory factory : serviceFactories)
                testService((TestService)factory.create(), sticky, deployC);
        }
    }

    /**
     *
     */
    interface DeployClosure {
        /**
         * @param services Services.
         * @param svcName Service name.
         * @param svc Service.
         * @throws Exception In case of an error.
         */
        void run(IgniteServices services, String svcName, TestService svc) throws Exception;
    }

    /**
     * Tests deployment and contract.
     *
     * @param svc Service.
     * @param sticky Sticky.
     * @param deployC Closure.
     * @throws Exception If failed.
     */
    protected void testService(TestService svc, boolean sticky, DeployClosure deployC) throws Exception {
        IgniteServices services;
        IgniteEx ignite = testedGrid();

        services = ignite.services();

        try {
            Object expected = value(++cntr);

            // Put value for testing Service instance serialization.
            svc.setValue(expected);

            deployC.run(services, SERVICE_NAME, svc);

            // Expect correct value from local instance.
            assertEquals(expected, svc.getValue());

            // Use stickiness to make sure data will be fetched from the same instance.
            TestService proxy = services.serviceProxy(SERVICE_NAME, TestService.class, sticky);

            // Expect that correct value is returned from deployed instance.
            assertEquals(expected, proxy.getValue());

            expected = value(++cntr);

            // Change value.
            proxy.setValue(expected);

            // Expect correct value after being read back.
            int r = 1000;

            while (r-- > 0)
                assertEquals(expected, proxy.getValue());

            assertEquals("Expected 1 deployed service", 1, services.serviceDescriptors().size());
        }
        finally {
            // Randomize stop method invocation
            boolean tmp = ThreadLocalRandom.current().nextBoolean();

            if (tmp)
                services.cancelAll();
            else
                services.cancel(SERVICE_NAME);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isCompatible() throws Exception {
        switch (dataMode) {
            case SERIALIZABLE:
            case CUSTOM_SERIALIZABLE:
            case EXTERNALIZABLE:
                return true;
        }

        return false;
    }

    /**
     * @param services Ignite services.
     * @param srvcName Service name to wait.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void waitForServiceDeploymentIfNeeded(IgniteServices services,
        String srvcName) throws IgniteInterruptedCheckedException {
        if (!isEventDrivenServiceProcessorEnabled)
            GridTestUtils.waitForCondition(() -> services.service(srvcName) != null, DEPLOYMENT_WAIT_TIMEOUT);
    }

    /**
     * Test service
     */
    public interface TestService {
        /**
         * @param o argument to set.
         */
        void setValue(Object o);

        /**
         * @return Argument
         * @throws Exception If failed.
         */
        Object getValue() throws Exception;
    }

    /**
     * Implementation for {@link TestService}
     */
    public static class TestServiceImpl implements Service, TestService {
        /** Test value. */
        protected Object val;

        /**
         * Default constructor.
         */
        public TestServiceImpl() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object getValue() throws Exception {
            return val;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void setValue(Object val) {
            this.val = val;
        }
    }

    /**
     * Echo service, externalizable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestServiceImplExternalizable extends TestServiceImpl implements Externalizable {
        /**
         * Default constructor.
         */
        public TestServiceImplExternalizable() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readObject();
        }
    }
}
