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

package org.apache.ignite.transactions;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for all transaction related exceptions.
 * In case of {@link org.apache.ignite.cache.CacheAtomicityMode#TRANSACTIONAL} cache -
 * any method throwing this or nested exception have transactional behaviour
 * (it can be rolled back and not seen outside transaction before committed).
 * In case of {@link org.apache.ignite.cache.CacheAtomicityMode#ATOMIC} cache - every action is committed when it done.
 * <p>
 * Before doing main thing, all transactional methods (commit, rollback and close) must obtain a readLock from
 * context's gateway to prevent simultaneous actions which can break result of transaction.
 *
 * Then method delegates action to internal transaction representation which can fail in some cases and throw exception.
 *
 * Anyway, if there is success or fail, method must free gateway lock.
 * <p>
 * {@link TransactionDeadlockException} If deadlock detected within transaction.
 * {@link TransactionHeuristicException} If operation performs within transaction that entered an unknown state.
 * {@link TransactionOptimisticException} If operation with optimistic behavior failed.
 * {@link TransactionRollbackException} If operation performs within transaction that automatically rolled back.
 * {@link TransactionTimeoutException} If operation performs within transaction and timeout occurred.
 * {@link TransactionSerializationException} If operation performs within mvcc transaction and write conflict occurred.
 */
public class TransactionException extends IgniteException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Creates empty exception. */
    public TransactionException() {
        // No-op.
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public TransactionException(String msg) {
        super(msg);
    }

    /**
     * Creates new transaction exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public TransactionException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
