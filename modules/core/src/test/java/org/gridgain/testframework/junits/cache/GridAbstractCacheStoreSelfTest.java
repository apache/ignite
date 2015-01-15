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

package org.gridgain.testframework.junits.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Abstract cache store test.
 */
public abstract class GridAbstractCacheStoreSelfTest<T extends CacheStore<Object, Object>>
    extends GridCommonAbstractTest {
    /** */
    protected final T store;

    /** */
    protected TestThreadLocalCacheSession ses = new TestThreadLocalCacheSession();

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    protected GridAbstractCacheStoreSelfTest() throws Exception {
        super(false);

        store = store();

        inject(store);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStore() throws Exception {
        // Create dummy transaction
        IgniteTx tx = new DummyTx();

        ses.newSession(tx);

        store.put("k1", "v1");
        store.put("k2", "v2");

        store.txEnd(true);

        ses.newSession(null);

        assertEquals("v1", store.load("k1"));
        assertEquals("v2", store.load("k2"));
        assertNull(store.load("k3"));

        ses.newSession(tx);

        store.remove("k1");

        store.txEnd(true);

        ses.newSession(null);

        assertNull(store.load("k1"));
        assertEquals("v2", store.load("k2"));
        assertNull(store.load("k3"));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testRollback() throws IgniteCheckedException {
        IgniteTx tx = new DummyTx();

        ses.newSession(tx);

        // Put.
        store.put("k1", "v1");

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k1"));

        // Put all.
        assertNull(store.load("k2"));

        store.putAll(Collections.singletonMap("k2", "v2"));

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k2"));

        store.putAll(Collections.singletonMap("k3", "v3"));

        store.txEnd(true); // Commit.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals("v3", store.load("k3"));

        store.put("k4", "v4");

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k4"));

        assertEquals("v3", store.load("k3"));

        // Remove.
        store.remove("k3");

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals("v3", store.load("k3"));

        // Remove all.
        store.removeAll(Arrays.asList("k3"));

        store.txEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals("v3", store.load("k3"));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithTXNoCommit() throws IgniteCheckedException {
        doTestAllOps(new DummyTx(), false);
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithTXCommit() throws IgniteCheckedException {
        doTestAllOps(new DummyTx(), true);
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testAllOpsWithoutTX() throws IgniteCheckedException {
        doTestAllOps(null, false);
    }

    /**
     * @param tx Transaction.
     * @param commit Commit.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestAllOps(@Nullable IgniteTx tx, boolean commit) throws IgniteCheckedException {
        try {
            ses.newSession(tx);

            store.put("key1", "val1");

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertEquals("val1", store.load("key1"));

            Map<String, String> m = new HashMap<>();

            m.put("key2", "val2");
            m.put("key3", "val3");

            store.putAll(m);

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();
            }

            final AtomicInteger cntr = new AtomicInteger();

            if (tx == null || commit) {
                store.loadAll(Arrays.asList("key1", "key2", "key3", "no_such_key"), new CI2<Object, Object>() {
                    @Override public void apply(Object o, Object o1) {
                        if ("key1".equals(o))
                            assertEquals("val1", o1);

                        if ("key2".equals(o))
                            assertEquals("val2", o1);

                        if ("key3".equals(o))
                            assertEquals("val3", o1);

                        if ("no_such_key".equals(o))
                            fail();

                        cntr.incrementAndGet();
                    }
                });

                assertEquals(3, cntr.get());
            }

            store.removeAll(Arrays.asList("key2", "key3"));

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit) {
                assertNull(store.load("key2"));
                assertNull(store.load("key3"));
                assertEquals("val1", store.load("key1"));
            }

            store.remove("key1");

            if (tx != null && commit) {
                store.txEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertNull(store.load("key1"));
        }
        finally {
            if (tx != null) {
                store.txEnd(false);

                ses.newSession(null);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleMultithreading() throws Exception {
        final Random rnd = new Random();

        final LinkedBlockingQueue<UUID> queue = new LinkedBlockingQueue<>();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 1000; i++) {
                    IgniteTx tx = rnd.nextBoolean() ? new DummyTx() : null;

                    ses.newSession(tx);

                    int op = rnd.nextInt(10);

                    boolean queueEmpty = false;

                    if (op < 4) { // Load.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                assertNotNull(store.load(key));
                            else {
                                final AtomicInteger cntr = new AtomicInteger();

                                store.loadAll(Collections.singleton(key), new CI2<Object, Object>() {
                                    @Override public void apply(Object o, Object o1) {
                                        cntr.incrementAndGet();

                                        assertNotNull(o);
                                        assertNotNull(o1);

                                        UUID key = (UUID) o;
                                        UUID val = (UUID) o1;

                                        assertTrue(key.equals(val) ||
                                            (key.getMostSignificantBits() == val.getLeastSignificantBits() &&
                                                key.getLeastSignificantBits() == val.getMostSignificantBits()));
                                    }
                                });

                                assertEquals(1, cntr.get());
                            }

                            if (tx != null)
                                store.txEnd(true);

                            queue.add(key);
                        }
                    }
                    else if (op < 6) { // Remove.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                store.remove(key);
                            else
                                store.removeAll(Collections.singleton(key));

                            if (tx != null)
                                store.txEnd(true);
                        }
                    }
                    else { // Update.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            UUID val = new UUID(key.getLeastSignificantBits(), key.getMostSignificantBits());

                            if (rnd.nextBoolean())
                                store.put(key, val);
                            else
                                store.putAll(Collections.singletonMap(key, val));

                            if (tx != null)
                                store.txEnd(true);

                            queue.add(key);
                        }
                    }

                    if (queueEmpty) { // Add.
                        UUID key = UUID.randomUUID();

                        if (rnd.nextBoolean())
                            store.put(key, key);
                        else
                            store.putAll(Collections.singletonMap(key, key));

                        if (tx != null)
                            store.txEnd(true);

                        queue.add(key);
                    }
                }

                return null;
            }
        }, 37);
    }

    /**
     * @param store Store.
     * @throws Exception If failed.
     */
    protected void inject(T store) throws Exception {
        getTestResources().inject(store);

        getTestResources().resources().injectBasicResource(store, IgniteCacheSessionResource.class, ses);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.startLifecycleAware(F.asList(store));

        final Collection<Object> keys = new ArrayList<>();

        CI2<Object, Object> c = new CI2<Object, Object>() {
            @Override
            public void apply(Object k, Object v) {
                keys.add(k);
            }
        };

        store.loadCache(c);

        if (keys.isEmpty())
            return;

        ses.newSession(null);

        store.removeAll(keys);

        keys.clear();

        store.loadCache(c);

        assertTrue(keys.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.stopLifecycleAware(log, F.asList(store));
    }

    /**
     * @return Store.
     */
    protected abstract T store();

    /**
     * Dummy transaction for test purposes.
     */
    public static class DummyTx extends GridMetadataAwareAdapter implements IgniteTx {
        /** */
        private final IgniteUuid xid = IgniteUuid.randomUuid();

        /** {@inheritDoc} */
        @Nullable @Override public IgniteUuid xid() {
            return xid;
        }

        /** {@inheritDoc} */
        @Nullable @Override public UUID nodeId() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long threadId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteTxIsolation isolation() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteTxConcurrency concurrency() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean implicit() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isInvalidate() {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteTxState state() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long timeout(long timeout) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean setRollbackOnly() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void commit() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteAsyncSupport enableAsync() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isAsync() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public <R> IgniteFuture<R> future() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IgniteCheckedException {
            // No-op.
        }
    }
}
