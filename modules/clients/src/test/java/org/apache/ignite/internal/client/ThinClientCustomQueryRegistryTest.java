/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.CustomQueryProcessor;
import org.apache.ignite.internal.processors.platform.client.ThinClientCustomQueryRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ThinClientCustomQueryRegistry.
 */
public class ThinClientCustomQueryRegistryTest {
    /** */
    private static final AtomicInteger TEST_VAL = new AtomicInteger();

    /** */
    public static final String TEST_QUERY_PROCESSOR_ID = "TEST_QUERY_PROCESSOR";

    /** */
    CustomQueryProcessor qp1 = new TestQueryProcessor(1);

    /** */
    CustomQueryProcessor qp2 = new TestQueryProcessor(2);

    /** */
    @Before
    public void setUp() throws Exception {
        TEST_VAL.set(0);
    }

    /** */
    @After
    public void tearDown() throws Exception {
        ThinClientCustomQueryRegistry.unregister(qp1);
        ThinClientCustomQueryRegistry.unregister(qp2);
    }

    /** */
    @Test
    public void testRegisterAndCall() {
        ThinClientCustomQueryRegistry.registerIfAbsent(qp1);
        ThinClientCustomQueryRegistry.call(0, TEST_QUERY_PROCESSOR_ID, (byte) 0, null);
        assertEquals(1, TEST_VAL.get());
    }

    /** */
    @Test
    public void testRepeatedRegister() {
        assertTrue(ThinClientCustomQueryRegistry.registerIfAbsent(qp1));
        assertFalse(ThinClientCustomQueryRegistry.registerIfAbsent(qp2));
    }

    /** */
    @Test
    public void testUregister() {
        ThinClientCustomQueryRegistry.registerIfAbsent(qp1);
        ThinClientCustomQueryRegistry.unregister(qp1);
        ThinClientCustomQueryRegistry.registerIfAbsent(qp2);
        ThinClientCustomQueryRegistry.call(0, TEST_QUERY_PROCESSOR_ID, (byte) 0, null);
        assertEquals(2, TEST_VAL.get());
    }

    /** */
    private static class TestQueryProcessor implements CustomQueryProcessor {
        /** */
        private final int testValue;

        /** */
        public TestQueryProcessor(int testValue) {
            this.testValue = testValue;
        }

        /** {@inheritDoc} */
        @Override public String id() {
            return TEST_QUERY_PROCESSOR_ID;
        }

        /** {@inheritDoc} */
        @Override public ClientResponse call(long requestId, byte methodId, BinaryRawReader reader) {
            TEST_VAL.set(testValue);
            return new ClientResponse(testValue);
        }
    }
}
