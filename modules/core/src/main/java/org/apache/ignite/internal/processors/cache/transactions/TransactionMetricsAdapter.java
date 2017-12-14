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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionMetrics;

/**
 * Tx metrics adapter.
 */
public class TransactionMetricsAdapter implements TransactionMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Number of transaction commits. */
    private AtomicInteger txCommits = new AtomicInteger();

    /** Number of transaction rollbacks. */
    private AtomicInteger txRollbacks = new AtomicInteger();

    /** Number of transaction rollbacks due to timeout. */
    private AtomicInteger txRollbacksOnTimeout = new AtomicInteger();

    /** Number of transaction rollbacks due to deadlock. */
    private AtomicInteger txRollbacksOnDeadlock = new AtomicInteger();


    /** Last commit time. */
    private volatile long commitTime;

    /** Last rollback time. */
    private volatile long rollbackTime;

    /**
     *
     */
    public TransactionMetricsAdapter() {

    }

    /**
     * @param m Transaction metrics to copy.
     */
    public TransactionMetricsAdapter(TransactionMetrics m) {
        commitTime = m.commitTime();
        rollbackTime = m.rollbackTime();
        txCommits.set(m.txCommits());
        txRollbacks.set(m.txRollbacks());
        txRollbacksOnTimeout.set(m.txRollbacksOnTimeout());
        txRollbacksOnDeadlock.set(m.txRollbacksOnDeadlock());
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime;
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits.intValue();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks.intValue();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacksOnTimeout() {
        return txRollbacksOnTimeout.intValue();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacksOnDeadlock() {
        return txRollbacksOnDeadlock.intValue();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime = U.currentTimeMillis();

        txCommits.incrementAndGet();
    }

    /**
     * Transaction rollback callback.
     *
     * @param timedOut If transaction was timed out.
     * @param deadlocked If transaction participated in the deadlock.
     */
    public void onTxRollback(boolean timedOut, boolean deadlocked) {
        rollbackTime = U.currentTimeMillis();

        txRollbacks.incrementAndGet();

        if (deadlocked)
            txRollbacksOnDeadlock.incrementAndGet();
        else if (timedOut)
            txRollbacksOnTimeout.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(commitTime);
        out.writeLong(rollbackTime);
        out.writeInt(txCommits.intValue());
        out.writeInt(txRollbacks.intValue());
        out.writeInt(txRollbacksOnTimeout.intValue());
        out.writeInt(txRollbacksOnDeadlock.intValue());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        commitTime = in.readLong();
        rollbackTime = in.readLong();
        txCommits.set(in.readInt());
        txRollbacks.set(in.readInt());
        txRollbacksOnTimeout.set(in.readInt());
        txRollbacksOnDeadlock.set(in.readInt());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionMetricsAdapter.class, this);
    }
}