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
import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * Full API services test.
 */
public class IgniteServiceConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Test service name. */
    private static final String SERVICE_NAME = "testService";

    /** Test service name. */
    private static final String CACHE_NAME = "testCache";

    /** */
    protected static final int CLIENT_NODE_IDX_2 = 4;

    /** Test object id counter */
    private static int counter;

    /** Callable factories. */
    @SuppressWarnings("unchecked")
    private static final Factory[] serviceFactories = new Factory[] {
        Parameters.factory(TestServiceImpl.class),

        Parameters.factory(TestServiceImplExternalizable.class),

        Parameters.factory(TestServiceImplBinarylizable.class)
    };

    /**
     * Test node singleton deployment
     */
    public void testNodeSingletonDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(true, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                services.deployNodeSingleton(svcName, (Service)svc);
            }
        }));
    }

    /**
     * Test cluster singleton deployment
     */
    public void testClusterSingletonDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                services.deployClusterSingleton(svcName, (Service)svc);
            }
        }));
    }

    /**
     * Test key affinity deployment
     */
    public void testKeyAffinityDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                IgniteCache<Object, Object> cache = grid(testedNodeIdx).getOrCreateCache(CACHE_NAME);

                services.deployKeyAffinitySingleton(svcName, (Service)svc, cache.getName(), "1");
            }
        }));
    }

    /**
     * Test multiple deployment
     */
    public void testMultipleDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(true, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                services.deployMultiple(svcName, (Service)svc, 0, 1);
            }
        }));
    }

    /**
     * Test deployment
     */
    public void testDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(false, new DeployClosure() {
            @Override public void run(IgniteServices services, String svcName, TestService svc) {
                services.deployClusterSingleton(svcName, (Service)svc);

                ServiceConfiguration cfg = new ServiceConfiguration();

                cfg.setName(svcName);

                cfg.setService((Service)svc);

                cfg.setTotalCount(1);

                cfg.setMaxPerNodeCount(1);

                cfg.setNodeFilter(services.clusterGroup().predicate());

                services.deploy(cfg);
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
         * Default constructor
         * @param sticky Sticky.
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
         * Deploy
         * @param services Services.
         * @param svcName Service name.
         * @param svc Service.
         */
        void run(IgniteServices services, String svcName, TestService svc);
    }

    /**
     * Tests deployment and contract.
     * @param svc Service.
     * @param sticky Sticky.
     */
    protected void testService(TestService svc, boolean sticky, DeployClosure deployC) throws Exception {
        IgniteEx ignite = testedGrid();

        IgniteServices services = ignite.services();

        Object expected = value(++counter);

        // Put value for testing Service instance serialization.
        svc.setValue(expected);

        deployC.run(services, SERVICE_NAME, svc);

        // Expect correct value from local instance.
        assertEquals(expected, svc.getValue());

        // Use stickiness to make sure data will be fetched from the same instance.
        TestService proxy = services.serviceProxy(SERVICE_NAME, TestService.class, sticky);

        // Expect that correct value is returned from deployed instance.
        assertEquals(expected, proxy.getValue());

        expected = value(++counter);

        // Change value.
        proxy.setValue(expected);

        // Expect correct value after being read back.
        int r = 1000;

        while(r-- > 0)
            assertEquals(expected, proxy.getValue());

        assertEquals("Expected 1 deployed service", 1, services.serviceDescriptors().size());

        // randomize stop method invocation
        boolean tmp = ThreadLocalRandom.current().nextBoolean();

        if (tmp)
            services.cancelAll();
        else
            services.cancel(SERVICE_NAME);
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
         * @return argument
         */
        Object getValue() throws Exception;
    }

    /**
     * Implementation for {@link TestService}
     */
    public static class TestServiceImpl implements Service, TestService, Serializable {
        /** Test value. */
        protected Object value;

        /**
         * Default constructor.
         */
        public TestServiceImpl() {
        }

        /** {@inheritDoc} */
        @Override public Object getValue() throws Exception {
            return value;
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
        public void setValue(Object arg) {
            this.value = arg;
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
            out.writeObject(value);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value = in.readObject();
        }
    }

    /**
     * Echo service, binarylizable object
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestServiceImplBinarylizable extends TestServiceImpl implements Binarylizable {
        /**
         * Default constructor.
         */
        public TestServiceImplBinarylizable() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeObject("arg", value);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            value = reader.readObject("arg");
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        int i = testsCfg.gridCount();

        if (i < 5)
            return super.expectedClient(testGridName);

        // Use two client nodes if grid counter 5 or greater.
        return getTestGridName(CLIENT_NODE_IDX).equals(testGridName) || getTestGridName(CLIENT_NODE_IDX_2).equals(testGridName);
    }
}
