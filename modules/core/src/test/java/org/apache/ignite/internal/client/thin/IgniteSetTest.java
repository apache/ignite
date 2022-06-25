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

import java.lang.reflect.Field;
import java.util.Objects;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests client set.
 * Partition awareness tests are in {@link ThinClientPartitionAwarenessStableTopologyTest#testIgniteSet()}.
 */
@SuppressWarnings("rawtypes")
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
            ClientIgniteSet<UserObj> clientSet = client.set("testUserObject", new ClientCollectionConfiguration());

            clientSet.add(new UserObj(1, "a"));

            assertTrue(clientSet.contains(new UserObj(1, "a")));
            assertFalse(clientSet.contains(new UserObj(1, "b")));
        }
    }

    @Test
    public void testUserObjectClientServer() {
        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<UserObj> clientSet = client.set("testUserObjectClientServer", new ClientCollectionConfiguration());

            // By default, Client sends obj as BinaryObject, resulting in a different behavior.
            // When thick and thin APIs are used with the same user-defined classes together,
            // it means that classes are available on the server, and we can deserialize the obj to enable matching behavior.
            clientSet.deserializeOnServer(true);

            IgniteSet<UserObj> serverSet = ignite(0).set(clientSet.name(), null);

            clientSet.add(new UserObj(1, "client"));
            serverSet.add(new UserObj(2, "server"));

            assertTrue(clientSet.contains(new UserObj(1, "client")));
            assertTrue(clientSet.contains(new UserObj(2, "server")));

            assertTrue(serverSet.contains(new UserObj(1, "client")));
            assertTrue(serverSet.contains(new UserObj(2, "server")));

            assertFalse(clientSet.contains(new UserObj(1, "x")));
            assertFalse(serverSet.contains(new UserObj(1, "x")));
        }
    }

    @Test
    public void testConfigPropagation() throws Exception {
        String groupName = "grp-testConfigPropagation";

        ClientCollectionConfiguration cfg = new ClientCollectionConfiguration()
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(7)
                .setCollocated(true)
                .setGroupName(groupName);

        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<UserObj> set = client.set("testConfigPropagation", cfg);

            GridCacheSetProxy serverSet = (GridCacheSetProxy) ignite(0).setNoCreate(set.name(), groupName);

            Field field = GridCacheSetProxy.class.getDeclaredField("cctx");
            field.setAccessible(true);
            GridCacheContext cctx = (GridCacheContext) field.get(serverSet);

            assertTrue(set.collocated());
            assertEquals(7, cctx.config().getBackups());
            assertEquals(CacheMode.PARTITIONED, cctx.config().getCacheMode());
            assertEquals(CacheAtomicityMode.TRANSACTIONAL, cctx.config().getAtomicityMode());
            assertEquals(groupName, cctx.config().getGroupName());
        }
    }

    @Test
    public void testSameNameInDifferentGroups() {
        String name = "testSameNameInDifferentGroups";
        ClientCollectionConfiguration cfg1 = new ClientCollectionConfiguration();

        ClientCollectionConfiguration cfg2 = new ClientCollectionConfiguration()
                .setGroupName("gp1");

        ClientCollectionConfiguration cfg3 = new ClientCollectionConfiguration()
                .setGroupName("gp2")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<Integer> set1 = client.set(name, cfg1);
            ClientIgniteSet<Integer> set2 = client.set(name, cfg2);
            ClientIgniteSet<Integer> set3 = client.set(name, cfg3);

            set1.add(1);
            set2.add(2);
            set3.add(3);

            assertTrue(set1.contains(1));
            assertTrue(set2.contains(2));
            assertTrue(set3.contains(3));

            assertFalse(set1.contains(2));
            assertFalse(set2.contains(3));
            assertFalse(set3.contains(1));
        }
    }

    @Test
    public void testSameNameDifferentOptions() {
        // TODO: We can't rely only on name and group name.
        // Cache mode, atomicity, and backups are also important.
        // This whole process makes operations slow. We should use ResourceRegistry instead.
        // Resource approach is more complicated with affinity, but we can deal with that.`
        // OR adopt AtomicLong logic and ignore that noise, get rid of IgniteUuid.
        // OR somehow leverage known cache id! It will also be fast!
        // TODO: Looks like a similar issue exists in thick API - only name and groupName are used to locate a DS. Write a test.
        String name = "testSameNameDifferentOptions";
        ClientCollectionConfiguration cfg1 = new ClientCollectionConfiguration()
                .setGroupName("gp1");

        ClientCollectionConfiguration cfg2 = new ClientCollectionConfiguration()
                .setGroupName("gp1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        try (IgniteClient client = startClient(0)) {
            ClientIgniteSet<Integer> set1 = client.set(name, cfg1);
            ClientIgniteSet<Integer> set2 = client.set(name, cfg2);

            set1.add(2);
            set2.add(3);

            assertTrue(set1.contains(2));
            assertTrue(set2.contains(3));

            assertFalse(set1.contains(3));
            assertFalse(set2.contains(1));
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserObj userObj = (UserObj) o;
            return id == userObj.id && Objects.equals(val, userObj.val);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, val);
        }
    }
}
