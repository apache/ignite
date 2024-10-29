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
import java.util.concurrent.ExecutionException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Checks entry processor invocation for thin client.
 */
@RunWith(Parameterized.class)
public class InvokeTest extends AbstractThinClientTest {
    /** */
    private static final int NODE_CNT = 3;

    /** Client. */
    private static IgniteClient client;

    /** Client. */
    private static ClientCache<Integer, Object> cache;

    /** */
    @Parameterized.Parameter
    public boolean atomic;

    /** */
    @Parameterized.Parameters(name = "Atomic: {0}")
    public static Collection<Object> params() {
        return F.asList(true, false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_CNT);

        client = startClient(0);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomic ? CacheAtomicityMode.ATOMIC : CacheAtomicityMode.TRANSACTIONAL)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        client.destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * Test cache invoke operations, simple case.
     */
    @Test
    public void testInvokeSimpleCase() {
        assertEquals(0, (int)cache.invoke(0, new IncrementProcessor()));
        assertEquals(1, (int)cache.invoke(0, new IncrementProcessor()));
    }

    /**
     * Test cache invokeAll operations, simple case.
     */
    @Test
    public void testInvokeAllSimpleCase() {
        Map<Integer, EntryProcessorResult<Integer>> map = cache.invokeAll(new HashSet<>(
            F.asList(0, 1)), new IncrementProcessor());

        assertEquals(2, map.size());
        assertEquals((Integer)0, map.get(0).get());
        assertEquals((Integer)0, map.get(1).get());

        map = cache.invokeAll(new HashSet<>(F.asList(1, 2)), new IncrementProcessor());
        assertEquals(2, map.size());
        assertEquals((Integer)1, map.get(1).get());
        assertEquals((Integer)0, map.get(2).get());
    }

    /**
     * Test async cache invoke/invokeAll.
     */
    @Test
    public void testAsync() throws Exception {
        assertEquals(3, cache.invokeAsync(0, new TestEntryProcessor(), 1, 2, 3).get());
        assertEquals(2, cache.get(0));

        Map<Integer, EntryProcessorResult<Object>> map = cache.invokeAllAsync(new HashSet<>(
            F.asList(0, 1)), new TestEntryProcessor(), 1, 2, 3).get();

        assertEquals(2, map.size());
        assertEquals(3, map.get(0).get());
        assertEquals(3, map.get(1).get());
        assertEquals(1, cache.get(0));
        assertEquals(2, cache.get(1));
    }

    /**
     * Test exception handling.
     */
    @Test
    public void testExceptionHandling() {
        try {
            cache.invoke(0, new FailingEntryProcessor());
            fail();
        }
        catch (EntryProcessorException e) {
            assertTrue("Failed".equals(e.getMessage()));
        }

        Map<Integer, EntryProcessorResult<Object>> res = cache.invokeAll(
            new HashSet<>(F.asList(0, 1)), new FailingEntryProcessor());

        assertEquals(2, res.size());

        try {
            res.get(0).get();
            fail();
        }
        catch (EntryProcessorException e) {
            assertTrue("Failed".equals(e.getMessage()));
        }

        try {
            res.get(1).get();
            fail();
        }
        catch (EntryProcessorException e) {
            assertTrue("Failed".equals(e.getMessage()));
        }
    }

    /**
     * Test exception handling by async operations.
     */
    @Test
    public void testAsyncExceptionHandling() throws Exception {
        IgniteClientFuture<?> fut = cache.invokeAsync(0, new FailingEntryProcessor());

        try {
            fut.get();
            fail();
        }
        catch (ExecutionException e) {
            assertTrue(X.hasCause(e, "Failed", EntryProcessorException.class));
        }

        Map<Integer, EntryProcessorResult<Object>> res = cache.invokeAllAsync(
            new HashSet<>(F.asList(0, 1)), new FailingEntryProcessor()).get();

        assertEquals(2, res.size());

        try {
            res.get(0).get();
            fail();
        }
        catch (EntryProcessorException e) {
            assertTrue("Failed".equals(e.getMessage()));
        }

        try {
            res.get(1).get();
            fail();
        }
        catch (EntryProcessorException e) {
            assertTrue("Failed".equals(e.getMessage()));
        }
    }

    /**
     * Test withKeepBinary flag.
     */
    @Test
    public void testWithKeepBinary() {
        Person person = new Person(0, "name");

        ClientCache<Integer, Object> keepBinaryCache = cache.withKeepBinary();
        Object res = keepBinaryCache.invoke(0, new BinaryObjectEntryProcessor(), person);

        assertEquals(client.binary().toBinary(person), res);

        try {
            cache.invoke(0, new BinaryObjectEntryProcessor(), person);
            fail();
        }
        catch (EntryProcessorException ignore) {
            // Expected.
        }
    }

    /**
     * Test arguments and result serialization.
     */
    @Test
    public void testSerialization() {
        checkSerialization(1, 2, 3);

        Person p1 = new Person(1, "name1");
        Person p2 = new Person(2, "name2");
        Person p3 = new Person(3, "name3");
        checkSerialization(p1, p2, p3);
        checkSerialization(new Object[] {p1, p2}, new Object[] {p2, p3}, new Object[] {p3, p1});
        checkSerialization(F.asList(p1, p2), F.asList(p2, p3), F.asList(p3, p1));
    }

    /** */
    private void checkSerialization(Object valIfExists, Object valIfNotExists, Object retVal) {
        // Remove keys.
        cache.invoke(0, new TestEntryProcessor(), null, null, null);
        cache.invokeAll(new HashSet<>(F.asList(0, 1)), new TestEntryProcessor(), null, null, null);

        Object res = cache.invoke(0, new TestEntryProcessor(), valIfExists, valIfNotExists, retVal);
        assertEqualsArraysAware(res, retVal);
        assertEqualsArraysAware(valIfNotExists, cache.get(0));

        cache.put(0, 0); // Reset value for existing key.
        cache.invoke(0, new TestEntryProcessor(), valIfExists, valIfNotExists, retVal);
        assertEqualsArraysAware(valIfExists, cache.get(0));

        cache.put(0, 0); // Reset value for existing key.
        Map<Integer, EntryProcessorResult<Object>> resMap = cache.invokeAll(new HashSet<>(F.asList(0, 1)),
            new TestEntryProcessor(), valIfExists, valIfNotExists, retVal);
        assertEquals(2, resMap.size());
        assertEqualsArraysAware(retVal, resMap.get(0).get());
        assertEqualsArraysAware(retVal, resMap.get(1).get());
        assertEqualsArraysAware(valIfExists, cache.get(0));
        assertEqualsArraysAware(valIfNotExists, cache.get(1));
    }

    /**
     * Test that invoke/invokeAll is transactional.
     */
    @Test
    public void testExplicitTx() {
        Assume.assumeFalse(atomic);

        try (ClientTransaction tx = client.transactions().txStart()) {
            cache.invoke(0, new TestEntryProcessor(), 1, 2, 3);
            assertEquals(2, cache.get(0));

            cache.invoke(0, new TestEntryProcessor(), 1, 2, 3);
            assertEquals(1, cache.get(0));

            cache.invokeAll(new HashSet<>(F.asList(0, 1)), new TestEntryProcessor(), 1, 2, 3);

            assertEquals(F.asMap(0, 1, 1, 2), cache.getAll(new HashSet<>(F.asList(0, 1))));

            tx.rollback();
        }

        assertFalse(cache.containsKey(0));
        assertFalse(cache.containsKey(1));

        try (ClientTransaction tx = client.transactions().txStart()) {
            cache.invoke(0, new TestEntryProcessor(), 1, 2, 3);
            cache.invokeAll(new HashSet<>(F.asList(0, 1)), new TestEntryProcessor(), 1, 2, 3);

            tx.commit();
        }

        assertEquals(F.asMap(0, 1, 1, 2), cache.getAll(new HashSet<>(F.asList(0, 1))));

    }

    /** */
    protected static class IncrementProcessor implements EntryProcessor<Integer, Object, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Object> e, Object... arguments) {
            Integer val = (Integer)e.getValue();

            e.setValue(val == null ? 0 : val + 1);

            return (Integer)e.getValue();
        }
    }

    /** */
    protected static class TestEntryProcessor implements EntryProcessor<Integer, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> e, Object... arguments) {
            // arguments[0] - value if exists.
            // arguments[1] - value if not exists.
            // arguments[2] - returned value.
            if (arguments == null || arguments.length < 3)
                throw new EntryProcessorException("Unexpected arguments: " + Arrays.toString(arguments));

            if (arguments[0] == null)
                e.remove();
            else
                e.setValue(e.exists() ? arguments[0] : arguments[1]);

            return arguments[2];
        }
    }

    /** */
    protected static class FailingEntryProcessor implements EntryProcessor<Integer, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> e, Object... arguments) {
            throw new EntryProcessorException("Failed");
        }
    }

    /** */
    protected static class BinaryObjectEntryProcessor implements EntryProcessor<Integer, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> e, Object... arguments) {
            if (arguments == null || arguments.length < 1 || !(arguments[0] instanceof BinaryObject))
                throw new EntryProcessorException("Expected binary object argument");

            return arguments[0];
        }
    }
}
