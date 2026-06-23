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

package org.apache.ignite.internal.processors.cache.jta;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;

/**
 * Minimal mock JTA TransactionManager and UserTransaction for tests.
 * Delegates XA operations to the enlisted XAResource but skips recovery registration.
 */
public class TestTransactionManager implements TransactionManager, UserTransaction {
    /** ctx field of CacheJtaResource (cached for performance). */
    private static final Field CACHETA_RES_CTX_FIELD;

    static {
        try {
            Field f = CacheJtaResource.class.getDeclaredField("ctx");
            f.setAccessible(true);
            CACHETA_RES_CTX_FIELD = f;
        }
        catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Active transaction per thread. */
    private final ThreadLocal<TestTransaction> threadTx = new ThreadLocal<>();

    @Override public int getStatus() throws SystemException {
        TestTransaction tx = threadTx.get();
        return tx == null ? jakarta.transaction.Status.STATUS_NO_TRANSACTION : tx.status;
    }

    @Override public Transaction getTransaction() throws SystemException {
        return threadTx.get();
    }

    @Override public void setTransactionTimeout(int seconds) throws SystemException {
        // No-op.
    }

    @Override public void begin() throws NotSupportedException, SystemException {
        threadTx.set(new TestTransaction());
    }

    @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException {
        TestTransaction tx = threadTx.get();
        if (tx == null)
            return;

        tx.commit();

        clearIgniteThreadMap(tx);

        threadTx.remove();
    }

    @Override public void rollback() throws SystemException {
        TestTransaction tx = threadTx.get();
        if (tx != null)
            tx.rollback();

        clearIgniteThreadMap(tx);

        threadTx.remove();
    }

    /**
     * Clears Ignite thread-local transaction map after JTA transaction completion.
     * <p>
     * Handles both XA path (via enlisted XAResource) and Synchronization path
     * (via registered Synchronization callbacks).
     * This is needed because {@code commitTxAsync()} does not clear the thread map on commit.
     */
    private void clearIgniteThreadMap(TestTransaction tx) {
        // Try XA path first.
        XAResource rsrc = tx.rsrc;

        if (rsrc instanceof CacheJtaResource) {
            clearThreadMapForResource((CacheJtaResource)rsrc);
            return;
        }

        // Try Synchronization path (useJtaSynchronization=true).
        for (Synchronization synch : tx.synchs) {
            if (synch instanceof CacheJtaResource)
                clearThreadMapForResource((CacheJtaResource)synch);
        }
    }

    /**
     * Clears thread map for given JTA resource.
     * Uses reflection to access private {@code ctx} field of {@link CacheJtaResource}.
     */
    private void clearThreadMapForResource(CacheJtaResource cacheJtaRes) {
        GridNearTxLocal igniteTx = cacheJtaRes.cacheTx();

        if (igniteTx != null) {
            try {
                GridKernalContext kctx = (GridKernalContext)CACHETA_RES_CTX_FIELD.get(cacheJtaRes);

                if (kctx != null)
                    kctx.cache().context().tm().clearThreadMap(igniteTx);
            }
            catch (Exception ignored) {
                // Should never happen.
            }
        }
    }

    @Override public void setRollbackOnly() throws SystemException {
        TestTransaction tx = threadTx.get();
        if (tx != null)
            tx.rollbackOnly = true;
    }

    @Override public Transaction suspend() throws SystemException {
        TestTransaction tx = threadTx.get();
        threadTx.remove();
        return tx;
    }

    @Override public void resume(Transaction tx) throws IllegalStateException, SystemException {
        if (tx instanceof TestTransaction)
            threadTx.set((TestTransaction)tx);
        else
            throw new IllegalStateException("Unknown transaction: " + tx);
    }

    /**
     * Minimal in-memory JTA transaction.
     */
    static class TestTransaction implements Transaction {
        /** Status. */
        int status = jakarta.transaction.Status.STATUS_ACTIVE;

        /** XAResource enlisted in this transaction. */
        volatile XAResource rsrc;

        /** Xid. */
        private final Xid xid = new TestXid();

        /** Rollback-only flag. */
        private volatile boolean rollbackOnly;

        /** Registered synchronizations. */
        final List<Synchronization> synchs = Collections.synchronizedList(new ArrayList<>());

        /**
         * {@inheritDoc}
         */
        @Override public int getStatus() {
            return status;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void setRollbackOnly() {
            rollbackOnly = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean enlistResource(XAResource rsrc) {
            this.rsrc = rsrc;

            try {
                rsrc.start(xid, XAResource.TMNOFLAGS);
            }
            catch (XAException ignored) {
                // No-op.
            }

            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean delistResource(XAResource rsrc, int flag) {
            if (this.rsrc == rsrc) {
                this.rsrc = null;
                return true;
            }
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void registerSynchronization(Synchronization synch) {
            synchs.add(synch);
        }

        /**
         * {@inheritDoc}
         */
        @Override public void rollback() {
            if (rsrc != null) {
                try {
                    rsrc.end(xid, XAResource.TMSUSPEND);
                    rsrc.rollback(xid);
                }
                catch (XAException ignored) {
                    // No-op.
                }
            }

            // Call Synchronization callbacks on rollback.
            for (Synchronization synch : synchs) {
                try {
                    synch.afterCompletion(jakarta.transaction.Status.STATUS_ROLLEDBACK);
                }
                catch (RuntimeException e) {
                    // Ignore.
                }
            }

            status = jakarta.transaction.Status.STATUS_ROLLEDBACK;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException {
            if (rollbackOnly)
                throw new RollbackException();

            // Call beforeCompletion on all synchronizations.
            for (Synchronization synch : synchs) {
                synch.beforeCompletion();
            }

            if (rsrc != null) {
                try {
                    rsrc.end(xid, XAResource.TMSUCCESS);

                    int ret = rsrc.prepare(xid);

                    if (ret == XAResource.XA_OK)
                        rsrc.commit(xid, false);
                    else
                        rsrc.rollback(xid);
                }
                catch (XAException e) {
                    throw new RollbackException(e.toString());
                }
            }

            // Call afterCompletion on all synchronizations.
            for (Synchronization synch : synchs) {
                synch.afterCompletion(jakarta.transaction.Status.STATUS_COMMITTED);
            }

            status = jakarta.transaction.Status.STATUS_COMMITTED;
        }
    }

    /**
     * Minimal Xid implementation for tests.
     */
    private static class TestXid implements Xid {
        /** Format ID. */
        private final int formatId = 0;

        /** Global transaction ID. */
        private final byte[] globalTransactionId = String.valueOf(System.nanoTime()).getBytes();

        /** Branch qualifier. */
        private final byte[] branchQualifier = new byte[] { 0, 0, 0, 1 };

        /** {@inheritDoc} */
        @Override public int getFormatId() {
            return formatId;
        }

        /** {@inheritDoc} */
        @Override public byte[] getGlobalTransactionId() {
            return globalTransactionId;
        }

        /** {@inheritDoc} */
        @Override public byte[] getBranchQualifier() {
            return branchQualifier;
        }
    }
}
