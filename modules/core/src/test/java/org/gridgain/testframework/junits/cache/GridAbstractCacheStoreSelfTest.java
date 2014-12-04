/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Abstract cache store test.
 */
public abstract class GridAbstractCacheStoreSelfTest<T extends GridCacheStore<Object, Object>>
    extends GridCommonAbstractTest {
    /** */
    protected final T store;

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
        GridCacheTx tx = new DummyTx();

        store.put(tx, "k1", "v1");
        store.put(tx, "k2", "v2");

        store.txEnd(tx, true);

        assertEquals("v1", store.load(null, "k1"));
        assertEquals("v2", store.load(null, "k2"));
        assertNull(store.load( null, "k3"));

        store.remove(tx, "k1");

        store.txEnd(tx, true);

        assertNull(store.load(null, "k1"));
        assertEquals("v2", store.load(null, "k2"));
        assertNull(store.load(null, "k3"));
    }

    /**
     * @throws GridException if failed.
     */
    public void testRollback() throws GridException {
        GridCacheTx tx = new DummyTx();

        // Put.
        store.put(tx, "k1", "v1");

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, "k1"));

        // Put all.
        assertNull(store.load(tx, "k2"));

        store.putAll(tx, Collections.singletonMap("k2", "v2"));

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, "k2"));

        store.putAll(tx, Collections.singletonMap("k3", "v3"));

        store.txEnd(tx, true); // Commit.

        tx = new DummyTx();

        assertEquals("v3", store.load(tx, "k3"));

        store.put(tx, "k4", "v4");

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertNull(store.load(tx, "k4"));

        assertEquals("v3", store.load(tx, "k3"));

        // Remove.
        store.remove(tx, "k3");

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertEquals("v3", store.load(tx, "k3"));

        // Remove all.
        store.removeAll(tx, Arrays.asList("k3"));

        store.txEnd(tx, false); // Rollback.

        tx = new DummyTx();

        assertEquals("v3", store.load(tx, "k3"));
    }

    /**
     * @throws GridException if failed.
     */
    public void testAllOpsWithTXNoCommit() throws GridException {
        doTestAllOps(new DummyTx(), false);
    }

    /**
     * @throws GridException if failed.
     */
    public void testAllOpsWithTXCommit() throws GridException {
        doTestAllOps(new DummyTx(), true);
    }

    /**
     * @throws GridException if failed.
     */
    public void testAllOpsWithoutTX() throws GridException {
        doTestAllOps(null, false);
    }

    /**
     * @param tx Transaction.
     * @param commit Commit.
     * @throws GridException If failed.
     */
    private void doTestAllOps(@Nullable GridCacheTx tx, boolean commit) throws GridException {
        try {
            store.put(tx, "key1", "val1");

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit)
                assertEquals("val1", store.load(tx, "key1"));

            Map<String, String> m = new HashMap<>();

            m.put("key2", "val2");
            m.put("key3", "val3");

            store.putAll(tx, m);

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            final AtomicInteger cntr = new AtomicInteger();

            if (tx == null || commit) {
                store.loadAll(tx, Arrays.asList("key1", "key2", "key3", "no_such_key"), new CI2<Object, Object>() {
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

            store.removeAll(tx, Arrays.asList("key2", "key3"));

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit) {
                assertNull(store.load(tx, "key2"));
                assertNull(store.load(tx, "key3"));
                assertEquals("val1", store.load(tx, "key1"));
            }

            store.remove(tx, "key1");

            if (tx != null && commit) {
                store.txEnd(tx, true);

                tx = new DummyTx();
            }

            if (tx == null || commit)
                assertNull(store.load(tx, "key1"));

        }
        finally {
            if (tx != null)
                store.txEnd(tx, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleMultithreading() throws Exception {
        final Random rnd = new Random();

        final LinkedBlockingQueue<UUID> queue = new LinkedBlockingQueue<>();

        multithreaded(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                for (int i = 0; i < 1000; i++) {
                    GridCacheTx tx = rnd.nextBoolean() ? new DummyTx() : null;

                    int op = rnd.nextInt(10);

                    boolean queueEmpty = false;

                    if (op < 4) { // Load.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                assertNotNull(store.load(tx, key));
                            else {
                                final AtomicInteger cntr = new AtomicInteger();

                                store.loadAll(tx, Collections.singleton(key), new CI2<Object, Object>() {
                                    @Override
                                    public void apply(Object o, Object o1) {
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
                                store.txEnd(tx, true);

                            queue.add(key);
                        }
                    }
                    else if (op < 6) { // Remove.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            if (rnd.nextBoolean())
                                store.remove(tx, key);
                            else
                                store.removeAll(tx, Collections.singleton(key));

                            if (tx != null)
                                store.txEnd(tx, true);
                        }
                    }
                    else { // Update.
                        UUID key = queue.poll();

                        if (key == null)
                            queueEmpty = true;
                        else {
                            UUID val = new UUID(key.getLeastSignificantBits(), key.getMostSignificantBits());

                            if (rnd.nextBoolean())
                                store.put(tx, key, val);
                            else
                                store.putAll(tx, Collections.singletonMap(key, val));

                            if (tx != null)
                                store.txEnd(tx, true);

                            queue.add(key);
                        }
                    }

                    if (queueEmpty) { // Add.
                        UUID key = UUID.randomUUID();

                        if (rnd.nextBoolean())
                            store.put(tx, key, key);
                        else
                            store.putAll(tx, Collections.singletonMap(key, key));

                        if (tx != null)
                            store.txEnd(tx, true);

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

        store.removeAll(null, keys);

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
    public static class DummyTx extends GridMetadataAwareAdapter implements GridCacheTx {
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
        @Nullable @Override public GridCacheTxIsolation isolation() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridCacheTxConcurrency concurrency() {
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
        @Nullable @Override public GridCacheTxState state() {
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
        @Override public void commit() throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteFuture<GridCacheTx> commitAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws GridException {
            // No-op.
        }
    }
}
