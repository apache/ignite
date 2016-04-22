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
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * Full API services test.
 */
public class IgniteServiceConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Test service name. */
    private static final String SERVICE_NAME = "testService";

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
        runInAllDataModes(new ServiceTestRunnable(false));
    }

    /**
     * Test cluster singleton deployment
     */
    public void testClusterSingletonDeploy() throws Exception {
        runInAllDataModes(new ServiceTestRunnable(true));
    }

    /**
     * Service test closure
     */
    private class ServiceTestRunnable implements TestRunnable {
        /** Sticky. */
        private final boolean sticky;

        /**
         * Default constructor
         * @param sticky Sticky.
         */
        public ServiceTestRunnable(boolean sticky) {
            this.sticky = sticky;
        }

        /** {@inheritDoc} */
        @Override public void run() throws Exception {
            for (Factory factory : serviceFactories)
                testService((TestService)factory.create(), sticky);
        }
    }

    /**
     * Tests deployment and contract.
     * @param svc Service.
     * @param sticky Sticky.
     */
    protected void testService(TestService svc, boolean sticky) throws Exception {
        IgniteEx ignite = testedGrid();

        IgniteServices services = ignite.services();

        Object expected = value(++counter);

        // Put value for testing Service instance serialization.
        svc.setValue(expected);

        if (sticky)
            // Node singleton deployment requires stickiness to work correctly.
            services.deployNodeSingleton(SERVICE_NAME, (Service)svc);
        else
            // Cluster singleton deployment already provides stickiness.
            services.deployClusterSingleton(SERVICE_NAME, (Service)svc);

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

        services.cancelAll();
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
