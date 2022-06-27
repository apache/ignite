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

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientAutoCloseableIterator;
import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests client set.
 * Partition awareness tests are in {@link ThinClientPartitionAwarenessStableTopologyTest#testIgniteSet()}.
 */
@SuppressWarnings({"rawtypes", "ZeroLengthArrayAllocation", "ThrowableNotThrown"})
public class IgniteSetTest extends AbstractThinClientTest {
    static IgniteClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
        client = startClient(0);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setPartitionAwarenessEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        client.close();
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Test
    public void testGetNonExistentSetReturnsNull() {
        assertNull(client.set("non-existent", null));
    }

    @Test
    public void testCloseThenUseThrowsException() {
        ClientIgniteSet<Integer> set = client.set("testCloseThenUseThrowsException", new ClientCollectionConfiguration());
        ClientIgniteSet<Integer> set2 = client.set(set.name(), null);

        set.add(1);
        set.close();

        assertThrowsClosed(set);
        assertThrowsClosed(set2);

        assertTrue(set.removed());
        assertTrue(set2.removed());
    }

    @Test
    public void testCloseAndCreateWithSameName() {
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

    @Test
    public void testAddRemoveContains() {
        ClientIgniteSet<String> set = client.set("testBasicUsage", new ClientCollectionConfiguration());

        assertTrue(set.isEmpty());

        set.add("foo");
        set.add("bar");

        assertTrue(set.contains("foo"));
        assertTrue(set.contains("bar"));
        assertFalse(set.contains("baz"));

        set.remove("foo");
        assertFalse(set.contains("foo"));

        assertEquals(1, set.size());
    }

    @Test
    public void testAddAll() {
        ClientIgniteSet<Integer> set = client.set("testAddAll", new ClientCollectionConfiguration());

        assertTrue(set.addAll(ImmutableList.of(1, 3)));
        assertTrue(set.contains(1));
        assertFalse(set.contains(2));
        assertTrue(set.contains(3));
        assertEquals(2, set.size());

        assertTrue(set.addAll(ImmutableList.of(1, 2, 3)));
        assertTrue(set.contains(1));
        assertTrue(set.contains(2));
        assertTrue(set.contains(3));
        assertEquals(3, set.size());

        assertFalse(set.addAll(ImmutableList.of(2, 3)));
        assertFalse(set.addAll(ImmutableList.of(3)));
        assertFalse(set.addAll(ImmutableList.of()));

        assertEquals(3, set.size());
    }

    @Test
    @SuppressWarnings("SuspiciousMethodCalls")
    public void testContainsAll() {
        ClientIgniteSet<Integer> set = client.set("testContainsAll", new ClientCollectionConfiguration());
        set.addAll(ImmutableList.of(1, 2, 3));

        assertTrue(set.containsAll(ImmutableList.of(1)));
        assertTrue(set.containsAll(ImmutableList.of(1, 2)));
        assertTrue(set.containsAll(ImmutableList.of(2, 1)));
        assertTrue(set.containsAll(ImmutableList.of(3, 1, 2)));

        assertFalse(set.containsAll(ImmutableList.of()));
        assertFalse(set.containsAll(ImmutableList.of(0)));
        assertFalse(set.containsAll(ImmutableList.of(0, 1)));
        assertFalse(set.containsAll(ImmutableList.of(1, 2, 4)));
    }

    @Test
    @SuppressWarnings({"SlowAbstractSetRemoveAll", "SuspiciousMethodCalls"})
    public void testRemoveAll() {
        ClientIgniteSet<Integer> set = client.set("testRemoveAll", new ClientCollectionConfiguration());
        set.addAll(ImmutableList.of(1, 2, 3));

        assertFalse(set.removeAll(ImmutableList.of()));
        assertFalse(set.removeAll(ImmutableList.of(0)));
        assertFalse(set.removeAll(ImmutableList.of(0, 4)));

        assertEquals(3, set.size());

        assertTrue(set.removeAll(ImmutableList.of(5, 4, 3, 1, 0)));

        assertEquals(1, set.size());
        assertTrue(set.contains(2));
    }

    @Test
    @SuppressWarnings("SuspiciousMethodCalls")
    public void testRetainAll() {
        ClientIgniteSet<Integer> set = client.set("testRemoveAll", new ClientCollectionConfiguration());

        assertFalse(set.retainAll(ImmutableList.of()));

        set.addAll(ImmutableList.of(1, 2, 3));

        assertFalse(set.retainAll(ImmutableList.of(3, 2, 1, 4)));
        assertFalse(set.retainAll(ImmutableList.of(1, 2, 3)));
        assertEquals(3, set.size());

        assertTrue(set.retainAll(ImmutableList.of(1, 4, 7)));
        assertEquals(1, set.size());
        assertTrue(set.contains(1));

        // retainAll with empty list: clear the collection and get a boolean value indicating if it was empty or not.
        assertTrue(set.retainAll(ImmutableList.of()));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testUserObject() {
        ClientIgniteSet<UserObj> clientSet = client.set("testUserObject", new ClientCollectionConfiguration());

        UserObj obj1 = new UserObj(1, "a");
        UserObj obj2 = new UserObj(2, "a");

        clientSet.add(obj1);
        clientSet.add(obj2);

        assertTrue(clientSet.contains(obj1));
        assertTrue(clientSet.contains(new UserObj(1, "a")));
        assertTrue(clientSet.containsAll(ImmutableList.of(obj1, obj2)));

        assertFalse(clientSet.contains(new UserObj(1, "b")));
    }

    @Test
    public void testUserObjectClientServer() {
        ClientIgniteSet<UserObj> clientSet = client.set("testUserObjectClientServer", new ClientCollectionConfiguration());

        // By default, Client sends obj as BinaryObject, resulting in a different behavior.
        // When thick and thin APIs are used with the same user-defined classes together,
        // it means that classes are available on the server, and we can deserialize the obj to enable matching behavior.
        clientSet.serverKeepBinary(false);

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

    @Test
    public void testConfigPropagation() throws Exception {
        String groupName = "grp-testConfigPropagation";

        ClientCollectionConfiguration cfg = new ClientCollectionConfiguration()
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(7)
                .setCollocated(true)
                .setGroupName(groupName);

        CollectionConfiguration serverCfg = new CollectionConfiguration()
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(7)
                .setCollocated(true)
                .setGroupName(groupName);

        ClientIgniteSet<UserObj> set = client.set("testConfigPropagation", cfg);

        GridCacheSetProxy serverSet = (GridCacheSetProxy) ignite(0).set(set.name(), serverCfg);

        Field field = GridCacheSetProxy.class.getDeclaredField("cctx");
        field.setAccessible(true);
        GridCacheContext cctx = (GridCacheContext) field.get(serverSet);

        assertTrue(set.colocated());
        assertFalse(set.removed());
        assertEquals("testConfigPropagation", set.name());
        assertEquals(7, cctx.config().getBackups());
        assertEquals(CacheMode.PARTITIONED, cctx.config().getCacheMode());
        assertEquals(CacheAtomicityMode.TRANSACTIONAL, cctx.config().getAtomicityMode());
        assertEquals(groupName, cctx.config().getGroupName());
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

    @Test
    public void testSameNameDifferentOptions() {
        String name = "testSameNameDifferentOptions";
        ClientCollectionConfiguration cfg1 = new ClientCollectionConfiguration()
                .setGroupName("gp1");

        ClientCollectionConfiguration cfg2 = new ClientCollectionConfiguration()
                .setGroupName("gp1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ClientIgniteSet<Integer> set1 = client.set(name, cfg1);
        ClientIgniteSet<Integer> set2 = client.set(name, cfg2);

        set1.add(2);
        set2.add(3);

        assertTrue(set1.contains(2));
        assertTrue(set2.contains(3));

        assertFalse(set1.contains(3));
        assertFalse(set2.contains(1));
    }

    @Test
    public void testIteratorEmpty() {
        ClientIgniteSet<Integer> set = client.set("testIteratorEmpty", new ClientCollectionConfiguration());

        ClientAutoCloseableIterator<Integer> iterator = set.iterator();

        assertEquals(1024, set.pageSize());
        assertFalse(iterator.hasNext());
        GridTestUtils.assertThrows(null, iterator::next, NoSuchElementException.class, null);
    }

    @Test
    public void testCloseBeforeEnd() throws Exception {
        ClientIgniteSet<Integer> set = client.set("testCloseBeforeEnd", new ClientCollectionConfiguration());
        set.pageSize(1);

        ImmutableList<Integer> keys = ImmutableList.of(1, 2, 3);
        set.addAll(keys);

        ClientAutoCloseableIterator<Integer> iter = set.iterator();

        assertTrue(iter.hasNext());
        iter.close();

        assertFalse(iter.hasNext());
    }

    @Test
    public void testIteratorForeach() {
        ClientIgniteSet<Integer> set = client.set("testIteratorForeach", new ClientCollectionConfiguration());
        set.pageSize(2);

        ImmutableList<Integer> keys = ImmutableList.of(1, 2, 3);
        set.addAll(keys);

        for (Integer k : set)
            assertTrue(keys.contains(k));
    }

    @Test
    public void testModifyWhileIterating() {
        ClientIgniteSet<Integer> set = client.set("testModifyWhileIterating", new ClientCollectionConfiguration());
        set.pageSize(1);

        ImmutableList<Integer> keys = ImmutableList.of(1, 2, 3);
        set.addAll(keys);

        ClientAutoCloseableIterator<Integer> iterator = set.iterator();

        set.remove(3);
        assertTrue(keys.contains(iterator.next()));

        set.remove(2);
        assertTrue(keys.contains(iterator.next()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testToArrayEmpty() {
        ClientIgniteSet<Integer> set = client.set("testToArrayEmpty", new ClientCollectionConfiguration());

        assertEquals(0, set.toArray().length);
        assertEquals(0, set.toArray(new Integer[0]).length);
    }

    @Test
    public void testToArray() {
        for (int i = 1; i < 10; i++)
            testToArray(i);
    }

    public void testToArray(int pageSize) {
        ClientIgniteSet<Integer> set = client.set("testToArray", new ClientCollectionConfiguration());
        set.pageSize(pageSize);

        ImmutableList<Integer> keys = ImmutableList.of(1, 2, 3, 4, 5);
        set.addAll(keys);

        Integer[] resTyped = set.toArray(new Integer[0]);

        assertEquals(5, resTyped.length);

        for (Integer k : resTyped)
            assertTrue(keys.contains(k));

        Object[] resObjects = set.toArray();

        assertEquals(5, resObjects.length);

        for (Object k : resObjects)
            assertTrue(keys.contains((Integer) k));
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
