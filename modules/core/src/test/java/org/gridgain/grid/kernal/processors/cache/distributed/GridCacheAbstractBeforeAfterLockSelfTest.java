/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests before and after lock callbacks.
 */
public abstract class GridCacheAbstractBeforeAfterLockSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected abstract GridCacheDistributionMode distributionMode();

    /** {@inheritDoc} */
    @Override protected abstract GridCacheMode cacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticCommit() throws Exception {
        checkTx(true, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRollback() throws Exception {
        checkTx(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticCommit() throws Exception {
        checkTx(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRollback() throws Exception {
        checkTx(false, OPTIMISTIC);
    }

    /**
     * @param commit Whether to commit tx.
     * @param concurrency Tx concurrency.
     * @throws Exception If failed.
     */
    private void checkTx(boolean commit, GridCacheTxConcurrency concurrency) throws Exception {
        CollectBeforeLockClosure[] closures = new CollectBeforeLockClosure[gridCount()];
        CollectAfterUnlockClosure[] inClosures = new CollectAfterUnlockClosure[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            closures[i] = new CollectBeforeLockClosure();
            inClosures[i] = new CollectAfterUnlockClosure();

            GridCacheAdapter<Object, Object> cache = ((GridKernal)grid(i)).context().cache().internalCache();

            cache.beforePessimisticLock(closures[i]);
            cache.afterPessimisticUnlock(inClosures[i]);
        }

        // After callbacks are set, check transactions.
        GridCache<String, String> cache = grid(0).cache(null);

        // Implicit txs.
        if (commit && concurrency == PESSIMISTIC) {
            for (int i = 0; i < 10; i++)
                cache.put("key" + i, "val" + i);
        }

        // Check get outside tx.
        cache.get("non-lock");

        // Explicit tx.
        GridCacheTx tx = cache.txStart(concurrency, READ_COMMITTED);

        try {
            // Should not lock read entries.
            cache.get("non-lock");

            for (int k = 10; k < 30; k++)
                cache.put("key" + k, "val" + k);

            if (commit)
                tx.commit();
        }
        finally {
            tx.close();
        }

        // Explicit txs with REPEATABLE_READ.
        tx = cache.txStart(concurrency, REPEATABLE_READ);

        try {
            for (int k = 30; k < 50; k++)
                cache.get("key" + k);

            if (commit)
                tx.commit();
        }
        finally {
            tx.close();
        }

        Collection<Object> allLocked = new HashSet<>(50, 1.0f);

        // Check consistency of lock-unlock pairs.
        for (int i = 0; i < gridCount(); i++) {
            Collection<Object> locked = new ArrayList<>(closures[i].events());
            Collection<Object> unlocked = new ArrayList<>(inClosures[i].events());

            assertTrue("Different lock and unlock sets [idx=" + i + ", locked=" + locked + ", unlocked=" + unlocked,
                locked.containsAll(unlocked) && unlocked.containsAll(locked));

            for (Object o : locked)
                assertFalse("non-lock".equals(o));

            allLocked.addAll(locked);
        }

        if (concurrency == PESSIMISTIC) {
            for (int k = commit ? 0 : 10; k < 50; k++)
                assertTrue("Missing locked key: " + k, allLocked.contains("key" + k));
        }
        else
            assertTrue(allLocked.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLocks() throws Exception {
        CollectBeforeLockClosure[] closures = new CollectBeforeLockClosure[gridCount()];
        CollectAfterUnlockClosure[] inClosures = new CollectAfterUnlockClosure[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            closures[i] = new CollectBeforeLockClosure();
            inClosures[i] = new CollectAfterUnlockClosure();

            GridCacheAdapter<Object, Object> cache = ((GridKernal)grid(i)).context().cache().internalCache();

            cache.beforePessimisticLock(closures[i]);
            cache.afterPessimisticUnlock(inClosures[i]);
        }

        // After callbacks are set, check transactions.
        GridCache<String, String> cache = grid(0).cache(null);

        // Explicit locks.
        for (int i = 0; i < 10; i++)
            cache.lock("key" + i, 0L);

        try {
            // Check get outside tx.
            cache.get("non-lock");
        }
        finally {
            for (int i = 0; i < 10; i++)
                cache.unlock("key" + i);
        }

        U.sleep(1000);

        Collection<Object> allLocked = new HashSet<>(10, 1.0f);

        // Check consistency of lock-unlock pairs.
        for (int i = 0; i < gridCount(); i++) {
            Collection<Object> locked = new ArrayList<>(closures[i].events());
            Collection<Object> unlocked = new ArrayList<>(inClosures[i].events());

            assertTrue("Different lock and unlock sets [idx=" + i + ", locked=" + locked +
                ", unlocked=" + unlocked + ']', locked.containsAll(unlocked) &&unlocked.containsAll(locked));

            for (Object o : locked)
                assertFalse("non-lock".equals(o));

            allLocked.addAll(locked);
        }

        for (int k = 0; k < 10; k++)
            assertTrue("Missing locked key: " + k, allLocked.contains("key" + k));
    }

    /**
     * Collecting lock closure.
     */
    private static class CollectBeforeLockClosure
        implements GridBiClosure<Collection<Object>, Boolean, GridFuture<Object>> {
        /** Collected events. */
        private Collection<Object> evts = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override public GridFuture<Object> apply(Collection<Object> keys, Boolean inTx) {
            evts.addAll(keys);

            return null;
        }

        /**
         * @return Collected events.
         */
        public Collection<Object> events() {
            return evts;
        }
    }

    /**
     * Collecting unlock closure.
     */
    private static class CollectAfterUnlockClosure extends GridInClosure3<Object,
        Boolean, GridCacheOperation> {
        /** Collected events. */
        private Collection<Object> evts = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override public void apply(Object k, Boolean inTx, GridCacheOperation op) {
            evts.add(k);
        }

        /**
         * @return Collected events.
         */
        public Collection<Object> events() {
            return evts;
        }
    }
}
