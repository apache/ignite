/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for local transactions.
 */
public abstract class IgniteMvccTxMultiThreadedAbstractTest extends IgniteTxAbstractTest {
    /**
     * @return Thread count.
     */
    protected abstract int threadCount();

    /**
     /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableReadCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableReadRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkCommitMultithreaded(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
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
    protected void checkRollbackMultithreaded(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
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
}
