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

import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Tests client set.
 * Partition awareness tests are in {@link ThinClientPartitionAwarenessStableTopologyTest#testIgniteSet()}.
 */
public class IgniteSetTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setPartitionAwarenessEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Test
    public void testGetNonExistentSetReturnsNull() {
        try (IgniteClient client = startClient(0)) {
            assertNull(client.set("non-existent", null));
        }
    }

    @Test
    public void testCloseThenUseThrowsException() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<Integer> set = client.set("testCloseThenUseThrowsException", new ClientCollectionConfiguration());
            ClientIgniteSet<Integer> set2 = client.set(set.name(), null);

            set.add(1);
            set.close();

            assertThrowsClosed(set);
            assertThrowsClosed(set2);

            assertTrue(set.removed());
            assertTrue(set2.removed());
        }
    }

    @Test
    public void testCloseAndCreateWithSameName() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<Integer> oldSet = client.set("testCreateCloseCreateRemovesOldData", new ClientCollectionConfiguration());
            ClientIgniteSet<Integer> oldSet2 = client.set(oldSet.name(), null);

            oldSet.add(1);
            oldSet.close();

            ClientIgniteSet<Integer> newSet = client.set(oldSet.name(), new ClientCollectionConfiguration());

            assertEquals(0, newSet.size());

            assertTrue(oldSet.removed());
            assertTrue(oldSet2.removed());

            assertThrowsClosed(oldSet);
            assertThrowsClosed(oldSet2);
        }
    }

    @Test
    public void testBasicUsage() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<String> set = client.set("testBasicUsage", new ClientCollectionConfiguration());

            set.add("foo");
            set.add("bar");

            assertTrue(set.contains("foo"));
            assertTrue(set.contains("bar"));
            assertFalse(set.contains("baz"));

            set.remove("foo");
            assertFalse(set.contains("foo"));

            assertEquals(1, set.size());
        }
    }

    @Test
    public void testUserObject() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<UserObj> set = client.set("testUserObject", new ClientCollectionConfiguration());

            set.add(new UserObj(1, "a"));

            // Binary object equality does not require overriding equals/hashCode
            assertTrue(set.contains(new UserObj(1, "a")));
            assertFalse(set.contains(new UserObj(1, "b")));

            UserObj res = set.iterator().next();
            assertEquals(1, res.id);
            assertEquals("a", res.val);
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    private static void assertThrowsClosed(ClientIgniteSet<Integer> set) {
        String msg = "IgniteSet with name '" + set.name() + "' does not exist.";
        GridTestUtils.assertThrows(null, set::size, ClientException.class, msg);
    }

    private static class UserObj {
        public final int id;
        public final String val;

        public UserObj(int id, String val) {
            this.id = id;
            this.val = val;
        }
    }
}
