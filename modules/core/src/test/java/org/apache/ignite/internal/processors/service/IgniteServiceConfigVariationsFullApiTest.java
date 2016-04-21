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
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * Full API services test.
 */
public class IgniteServiceConfigVariationsFullApiTest extends IgniteConfigVariationsAbstractTest {
    /** Test service name. */
    private static final String SERVICE_NAME = "echo";

    /** Callable factories. */
    private static final Factory[] serviceFactories = new Factory[] {
        Parameters.factory(TestServiceImpl.class),
        Parameters.factory(TestServiceImplExternalizable.class),
        Parameters.factory(TestServiceImplBinarylizable.class)
    };

    /**
     * The test's wrapper runs the test with each factory from the factories array.
     *
     * @param test test object, a factory is passed as a parameter.
     * @param factories various factories
     * @throws Exception If failed.
     */
    private void runWithAllFactories(Factory[] factories, ServiceTest test) throws Exception {
        for (int i = 0; i < factories.length; i++) {
            Factory factory = factories[i];

            if (i != 0)
                beforeTest();

            try {
                test.test(factory, grid(testedNodeIdx));
            }
            finally {
                if (i + 1 != factories.length)
                    afterTest();
            }
        }
    }

    /**
     * The test's wrapper provides variations of the argument data model and user factories. The test is launched {@code
     * factories.length * DataMode.values().length} times.
     *
     * @param test Test.
     * @param factories various factories
     * @throws Exception If failed.
     */
    protected void runTest(final Factory[] factories, final ServiceTest test) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                try {
                    if ((getConfiguration().getMarshaller() instanceof JdkMarshaller)
                        && (dataMode == DataMode.PLANE_OBJECT)) {
                        info("Skip test for JdkMarshaller & PLANE_OBJECT data mode");
                        return;
                    }
                }
                catch (Exception e) {
                    assert false : e.getMessage();
                }

                runWithAllFactories(factories, test);
            }
        });
    }

    /**
     * Test cluster node singleton deployment
     */
    public void testNodeSingletonDeploy() throws Exception {
        runTest(serviceFactories, new ServiceTest() {
            @Override public void test(Factory factory, IgniteEx ignite) throws Exception {
                IgniteServices services = ignite.services();

                TestServiceImpl svc = (TestServiceImpl)factory.create();

                Object exp = value(1);

                // put value for testing Service instance serialization
                svc.setValue(exp);

                services.deployNodeSingleton(SERVICE_NAME, svc);

                // expect correct value from local instance
                assertEquals(exp, svc.getValue());

                TestService stickyEcho = services.serviceProxy(SERVICE_NAME, TestService.class, true);

                // expect correct value from deployed instance
                assertEquals(exp, stickyEcho.getValue());

                exp = value(2);

                // change value
                stickyEcho.setValue(exp);

                // expect correct value after being read back
                assertEquals(exp, stickyEcho.getValue());

                services.cancelAll();
            }
        });
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
        };

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op
            System.out.println();
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

    /**
     * Service test closure
     */
    interface ServiceTest {
        /**
         * Runs test
         * @param factory Factory.
         * @param ignite Ignite.
         */
        void test(Factory factory, IgniteEx ignite) throws Exception;
    }
}
