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

package org.apache.ignite.internal.transactions.proxy;

/** Represents Ignite client-independent transaction operations. */
public interface TransactionProxy extends AutoCloseable {
    /** Commits this transaction. */
    public void commit();

    /** Rolls back this transaction. */
    public void rollback();

    /** Ends the transaction. Transaction will be rolled back if it has not been committed. */
    @Override public void close();

    /**
     * Modify the transaction associated with the current thread such that the
     * only possible outcome of the transaction is to roll back the
     * transaction.
     *
     * @return {@code True} if rollback-only flag was set as a result of this operation,
     *      {@code false} if it was already set prior to this call or could not be set
     *      because transaction is already finishing up committing or rolling back.
     */
    public boolean setRollbackOnly();
}
