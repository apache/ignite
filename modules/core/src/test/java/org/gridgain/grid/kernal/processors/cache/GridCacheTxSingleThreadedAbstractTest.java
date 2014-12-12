/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests for local transactions.
 */
@SuppressWarnings( {"BusyWait"})
public abstract class GridCacheTxSingleThreadedAbstractTest extends GridCacheTxAbstractTest {
    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedCommit() throws Exception {
        checkCommit(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadCommit() throws Exception {
        checkCommit(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableCommit() throws Exception {
        checkCommit(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void _testOptimisticReadCommittedCommit() throws Exception { // TODO GG-9141
        checkCommit(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadCommit() throws Exception {
        checkCommit(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableCommit() throws Exception {
        checkCommit(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedRollback() throws Exception {
        checkRollback(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadRollback() throws Exception {
        checkRollback(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableRollback() throws Exception {
        checkRollback(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommittedRollback() throws Exception {
        checkRollback(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadRollback() throws Exception {
        checkRollback(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableRollback() throws Exception {
        checkRollback(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }
}
