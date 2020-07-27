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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks service invocation for thin client.
 */
public class ServicesTest extends AbstractThinClientTest {
    /** Node-id service name. */
    private static final String NODE_ID_SERVICE_NAME = "node_id_svc";

    /** Node-singleton service name. */
    private static final String NODE_SINGLTON_SERVICE_NAME = "node_svc";

    /** Cluster-singleton service name. */
    private static final String CLUSTER_SINGLTON_SERVICE_NAME = "cluster_svc";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        startClientGrid(3);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        grid(0).services().deployNodeSingleton(NODE_ID_SERVICE_NAME, new TestNodeIdService());

        grid(0).services().deployNodeSingleton(NODE_SINGLTON_SERVICE_NAME, new TestService());

        // Deploy CLUSTER_SINGLTON_SERVICE_NAME to grid(1).
        int keyGrid1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        grid(0).services().deployKeyAffinitySingleton(CLUSTER_SINGLTON_SERVICE_NAME, new TestService(),
            DEFAULT_CACHE_NAME, keyGrid1);
    }

    /**
     * Test that overloaded methods resolved correctly.
     */
    @Test
    public void testOverloadedMethods() throws Exception {
        try (IgniteClient client = startClient(0)) {
            // Test local service calls (service deployed to each node).
            TestServiceInterface svc = client.services().serviceProxy(NODE_SINGLTON_SERVICE_NAME,
                TestServiceInterface.class);

            checkOverloadedMethods(svc);

            // Test remote service calls (client connected to grid(0) but service deployed to grid(1)).
            svc = client.services().serviceProxy(CLUSTER_SINGLTON_SERVICE_NAME, TestServiceInterface.class);

            checkOverloadedMethods(svc);
        }
    }

    /**
     * @param svc Service.
     */
    private void checkOverloadedMethods(TestServiceInterface svc) {
        assertEquals("testMethod()", svc.testMethod());

        assertEquals("testMethod(String val): test", svc.testMethod("test"));

        assertEquals(123, svc.testMethod(123));

        assertEquals("testMethod(Object val): test", svc.testMethod(new StringBuilder("test")));

        assertEquals("testMethod(String val): null", svc.testMethod((String)null));

        assertEquals("testMethod(Object val): null", svc.testMethod((Object)null));

        Person person1 = new Person(1, "Person 1");
        Person person2 = new Person(2, "Person 2");

        assertEquals("testMethod(Person person, Object obj): " + person1 + ", " + person2,
            svc.testMethod(person1, (Object)person2));

        assertEquals("testMethod(Object obj, Person person): " + person1 + ", " + person2,
            svc.testMethod((Object)person1, person2));
    }

    /**
     * Test that methods which get and return collections work correctly.
     */
    @Test
    public void testCollectionMethods() throws Exception {
        try (IgniteClient client = startClient(0)) {
            // Test local service calls (service deployed to each node).
            TestServiceInterface svc = client.services().serviceProxy(NODE_SINGLTON_SERVICE_NAME,
                TestServiceInterface.class);

            checkCollectionMethods(svc);

            // Test remote service calls (client connected to grid(0) but service deployed to grid(1)).
            svc = client.services().serviceProxy(CLUSTER_SINGLTON_SERVICE_NAME, TestServiceInterface.class);

            checkCollectionMethods(svc);
        }
    }

    /**
     * @param svc Service.
     */
    private void checkCollectionMethods(TestServiceInterface svc) {
        Person person1 = new Person(1, "Person 1");
        Person person2 = new Person(2, "Person 2");

        Person[] arr = new Person[] {person1, person2};
        assertTrue(Arrays.equals(arr, svc.testArray(arr)));

        Collection<Person> col = new HashSet<>(F.asList(person1, person2));
        assertEquals(col, svc.testCollection(col));

        Map<Integer, Person> map = F.asMap(1, person1, 2, person2);
        assertEquals(map, svc.testMap(map));
    }

    /**
     * Test that exception is thrown when invoking service with wrong interface.
     */
    @Test
    public void testWrongMethodInvocation() throws Exception {
        try (IgniteClient client = startClient(0)) {
            TestServiceInterface svc = client.services().serviceProxy(NODE_ID_SERVICE_NAME,
                TestServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, () -> svc.testMethod(0), ClientException.class,
                "Method not found");
        }
    }

    /**
     * Test that exception is thrown when trying to invoke non-existing service.
     */
    @Test
    public void testWrongServiceName() throws Exception {
        try (IgniteClient client = startClient(0)) {
            TestServiceInterface svc = client.services().serviceProxy("no_such_service",
                TestServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, () -> svc.testMethod(0), ClientException.class,
                "Service not found");
        }
    }

    /**
     * Test that service exception message is propagated to client.
     */
    @Test
    public void testServiceException() throws Exception {
        try (IgniteClient client = startClient(0)) {
            // Test local service calls (service deployed to each node).
            TestServiceInterface svc = client.services().serviceProxy(NODE_SINGLTON_SERVICE_NAME,
                TestServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, svc::testException, ClientException.class,
                "testException()");

            // Test remote service calls (client connected to grid(0) but service deployed to grid(1)).
            client.services().serviceProxy(CLUSTER_SINGLTON_SERVICE_NAME, TestServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, svc::testException, ClientException.class,
                "testException()");
        }
    }

    /**
     * Test that services executed on cluster group.
     */
    @Test
    public void testServicesOnClusterGroup() throws Exception {
        try (IgniteClient client = startClient(0)) {
            // Local node.
            ClientClusterGroup grp = client.cluster().forNodeId(nodeId(0), nodeId(3));

            TestNodeIdServiceInterface nodeSvc0 = client.services(grp).serviceProxy(NODE_ID_SERVICE_NAME,
                TestNodeIdServiceInterface.class);

            assertEquals(nodeId(0), nodeSvc0.nodeId());

            // Remote node.
            grp = client.cluster().forNodeId(nodeId(1), nodeId(3));

            nodeSvc0 = client.services(grp).serviceProxy(NODE_ID_SERVICE_NAME,
                TestNodeIdServiceInterface.class);

            assertEquals(nodeId(1), nodeSvc0.nodeId());

            // Client node.
            grp = client.cluster().forNodeId(nodeId(3));

            TestNodeIdServiceInterface nodeSvc1 = client.services(grp).serviceProxy(NODE_ID_SERVICE_NAME,
                TestNodeIdServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, nodeSvc1::nodeId, ClientException.class,
                "Failed to find deployed service");

            // All nodes, except service node.
            grp = client.cluster().forNodeId(nodeId(0), nodeId(2), nodeId(3));

            TestServiceInterface nodeSvc2 = client.services(grp).serviceProxy(CLUSTER_SINGLTON_SERVICE_NAME,
                TestServiceInterface.class);

            GridTestUtils.assertThrowsAnyCause(log, nodeSvc2::testMethod, ClientException.class,
                "Failed to find deployed service");
        }
    }

    /**
     * Test services timeout.
     */
    @Test
    public void testServiceTimeout() throws Exception {
        long timeout = 100L;

        try (IgniteClient client = startClient(0)) {
            TestServiceInterface svc = client.services().serviceProxy(CLUSTER_SINGLTON_SERVICE_NAME,
                TestServiceInterface.class, timeout);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> svc.sleep(timeout * 2));

            GridTestUtils.assertThrowsAnyCause(log, fut::get, ClientException.class, "timed out");
        }
    }

    /** */
    public static interface TestServiceInterface {
        /** */
        public String testMethod();

        /** */
        public String testMethod(String val);

        /** */
        public String testMethod(Object val);

        /** */
        public int testMethod(int val);

        /** */
        public String testMethod(Person person, Object obj);

        /** */
        public String testMethod(Object obj, Person person);

        /** */
        public Person[] testArray(Person[] persons);

        /** */
        public Collection<Person> testCollection(Collection<Person> persons);

        /** */
        public Map<Integer, Person> testMap(Map<Integer, Person> persons);

        /** */
        public Object testException();

        /** */
        public void sleep(long millis);
    }

    /**
     * Implementation of TestServiceInterface.
     */
    public static class TestService implements Service, TestServiceInterface {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String testMethod() {
            return "testMethod()";
        }

        /** {@inheritDoc} */
        @Override public String testMethod(String val) {
            return "testMethod(String val): " + val;
        }

        /** {@inheritDoc} */
        @Override public String testMethod(Object val) {
            return "testMethod(Object val): " + val;
        }

        /** {@inheritDoc} */
        @Override public int testMethod(int val) {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String testMethod(Person person, Object obj) {
            return "testMethod(Person person, Object obj): " + person + ", " + obj;
        }

        /** {@inheritDoc} */
        @Override public String testMethod(Object obj, Person person) {
            return "testMethod(Object obj, Person person): " + obj + ", " + person;
        }

        /** {@inheritDoc} */
        @Override public Person[] testArray(Person[] persons) {
            return persons;
        }

        /** {@inheritDoc} */
        @Override public Collection<Person> testCollection(Collection<Person> persons) {
            return persons;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Person> testMap(Map<Integer, Person> persons) {
            return persons;
        }

        /** {@inheritDoc} */
        @Override public Object testException() {
            throw new IllegalStateException("testException()");
        }

        /** {@inheritDoc} */
        @Override public void sleep(long millis) {
            long ts = U.currentTimeMillis();

            doSleep(millis);

            // Make sure cached timestamp updated.
            while (ts + millis >= U.currentTimeMillis())
                doSleep(100L);
        }
    }

    /**
     * Service to return ID of node where method was executed.
     */
    public static interface TestNodeIdServiceInterface {
        /** Gets ID of node where method was executed */
        public UUID nodeId();
    }

    /**
     * Implementation of TestNodeIdServiceInterface
     */
    public static class TestNodeIdService implements Service, TestNodeIdServiceInterface {
        /** Ignite. */
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public UUID nodeId() {
            return ignite.cluster().localNode().id();
        }
    }
}
