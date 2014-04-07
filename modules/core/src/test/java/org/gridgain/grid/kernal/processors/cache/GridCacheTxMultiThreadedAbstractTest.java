/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests for local transactions.
 */
@SuppressWarnings( {"BusyWait"})
public abstract class GridCacheTxMultiThreadedAbstractTest extends GridCacheTxAbstractTest {
    /**
     * @return Thread count.
     */
    protected abstract int threadCount();

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkCommitMultithreaded(final GridCacheTxConcurrency concurrency,
        final GridCacheTxIsolation isolation) throws Exception {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Thread t = Thread.currentThread();

                t.setName(t.getName() + "-id-" + t.getId());

                info("Starting commit thread: " + Thread.currentThread().getName());

                try {
                    checkCommit(concurrency, isolation);
                }
                finally {
                    info("Finished commit thread: " + Thread.currentThread().getName());
                }

                return null;
            }
        }, threadCount(), concurrency + "-" + isolation);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkRollbackMultithreaded(final GridCacheTxConcurrency concurrency,
        final GridCacheTxIsolation isolation) throws Exception {
        final ConcurrentMap<Integer, String> map = new ConcurrentHashMap<>();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Thread t = Thread.currentThread();

                t.setName(t.getName() + "-id-" + t.getId());

                info("Starting rollback thread: " + Thread.currentThread().getName());

                try {
                    checkRollback(map, concurrency, isolation);

                    return null;
                }
                finally {
                    info("Finished rollback thread: " + Thread.currentThread().getName());
                }
            }
        }, threadCount(), concurrency + "-" + isolation);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticReadCommittedCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticRepeatableReadCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticSerializableCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticReadCommittedCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticRepeatableReadCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticSerializableCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticReadCommittedRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticRepeatableReadRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticSerializableRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticReadCommittedRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticRepeatableReadRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws GridException If test failed.
     */
    public void testOptimisticSerializableRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }
}
