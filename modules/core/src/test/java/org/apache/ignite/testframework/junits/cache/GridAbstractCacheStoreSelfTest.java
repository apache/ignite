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

package org.apache.ignite.testframework.junits.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

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
        Transaction tx = new DummyTx();

        ses.newSession(tx);

        store.write(new CacheEntryImpl<>("k1", "v1"));
        store.write(new CacheEntryImpl<>("k2", "v2"));

        store.sessionEnd(true);

        ses.newSession(null);

        assertEquals("v1", store.load("k1"));
        assertEquals("v2", store.load("k2"));
        assertNull(store.load("k3"));

        ses.newSession(tx);

        store.delete("k1");

        store.sessionEnd(true);

        ses.newSession(null);

        assertNull(store.load("k1"));
        assertEquals("v2", store.load("k2"));
        assertNull(store.load("k3"));
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testRollback() throws IgniteCheckedException {
        Transaction tx = new DummyTx();

        ses.newSession(tx);

        // Put.
        store.write(new CacheEntryImpl<>("k1", "v1"));

        store.sessionEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k1"));

        // Put all.
        assertNull(store.load("k2"));

        Collection<Cache.Entry<? extends Object, ? extends Object>> col = new ArrayList<>();

        col.add(new CacheEntryImpl<>("k2", "v2"));

        store.writeAll(col);

        store.sessionEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k2"));

        col = new ArrayList<>();

        col.add(new CacheEntryImpl<>("k3", "v3"));

        store.writeAll(col);

        store.sessionEnd(true); // Commit.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals("v3", store.load("k3"));

        store.write(new CacheEntryImpl<>("k4", "v4"));

        store.sessionEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertNull(store.load("k4"));

        assertEquals("v3", store.load("k3"));

        // Remove.
        store.delete("k3");

        store.sessionEnd(false); // Rollback.

        tx = new DummyTx();

        ses.newSession(tx);

        assertEquals("v3", store.load("k3"));

        // Remove all.
        store.deleteAll(Arrays.asList("k3"));

        store.sessionEnd(false); // Rollback.

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
     */
    private void doTestAllOps(@Nullable Transaction tx, boolean commit) {
        try {
            ses.newSession(tx);

            store.write(new CacheEntryImpl<>("key1", "val1"));

            if (tx != null && commit) {
                store.sessionEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertEquals("val1", store.load("key1"));

            Collection<Cache.Entry<? extends Object, ? extends Object>> col = new ArrayList<>();

            col.add(new CacheEntryImpl<>("key2", "val2"));
            col.add(new CacheEntryImpl<>("key3", "val3"));

            store.writeAll(col);

            if (tx != null && commit) {
                store.sessionEnd(true);

                tx = new DummyTx();
            }

            if (tx == null || commit) {
                Map<Object, Object> loaded = store.loadAll(Arrays.asList("key1", "key2", "key3", "no_such_key"));

                for (Map.Entry<Object, Object> e : loaded.entrySet()) {
                    Object key = e.getKey();
                    Object val = e.getValue();

                    if ("key1".equals(key))
                        assertEquals("val1", val);

                    if ("key2".equals(key))
                        assertEquals("val2", val);

                    if ("key3".equals(key))
                        assertEquals("val3", val);

                    if ("no_such_key".equals(key))
                        fail();
                }

                assertEquals(3, loaded.size());
            }

            store.deleteAll(Arrays.asList("key2", "key3"));

            if (tx != null && commit) {
                store.sessionEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit) {
                assertNull(store.load("key2"));
                assertNull(store.load("key3"));
                assertEquals("val1", store.load("key1"));
            }

            store.delete("key1");

            if (tx != null && commit) {
                store.sessionEnd(true);

                tx = new DummyTx();

                ses.newSession(tx);
            }

            if (tx == null || commit)
                assertNull(store.load("key1"));
        }
        finally {
            if (tx != null) {
                store.sessionEnd(false);

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
                    Transaction tx = rnd.nextBoolean() ? new DummyTx() : null;

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
                                Map<Object, Object> loaded = store.loadAll(Collections.singleton(key));

                                assertEquals(1, loaded.size());

                                Map.Entry<Object, Object> e = loaded.entrySet().iterator().next();

                                UUID k = (UUID)e.getKey();
                                UUID v = (UUID)e.getValue();

                                assertTrue(k.equals(v) || (k.getMostSignificantBits() == v.getLeastSignificantBits()
                                    && k.getLeastSignificantBits() == v.getMostSignificantBits()));
                            }

                            if (tx != null)
                                store.sessionEnd(true);

                            queue.add(key);
                        }
                    }
                    else if (op < 6) { // Remove.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                store.delete(key);
                            else
                                store.deleteAll(Collections.singleton(key));

                            if (tx != null)
                                store.sessionEnd(true);
                        }
                    }
                    else { // Update.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            UUID val = new UUID(key.getLeastSignificantBits(), key.getMostSignificantBits());

                            if (rnd.nextBoolean())
                                store.write(new CacheEntryImpl<>(key, val));
                            else {
                                Collection<Cache.Entry<? extends Object, ? extends Object>> col = new ArrayList<>();

                                col.add(new CacheEntryImpl<>(key, val));

                                store.writeAll(col);
                            }

                            if (tx != null)
                                store.sessionEnd(true);

                            queue.add(key);
                        }
                    }

                    if (queueEmpty) { // Add.
                        UUID key = UUID.randomUUID();

                        if (rnd.nextBoolean())
                            store.write(new CacheEntryImpl<>(key, key));
                        else {
                            Collection<Cache.Entry<? extends Object, ? extends Object>> col = new ArrayList<>();

                            col.add(new CacheEntryImpl<>(key, key));

                            store.writeAll(col);
                        }

                        if (tx != null)
                            store.sessionEnd(true);

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

        GridTestUtils.setFieldValue(store, "ses", ses);
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

        store.deleteAll(keys);

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
    public static class DummyTx extends GridMetadataAwareAdapter implements Transaction {
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
        @Nullable @Override public TransactionIsolation isolation() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public TransactionConcurrency concurrency() {
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
        @Nullable @Override public TransactionState state() {
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
        @Override public void commit() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteAsyncSupport withAsync() {
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
        @Override public void rollback() {
            // No-op.
        }
    }
}