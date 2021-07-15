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

package org.apache.ignite.tx;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Ignite Transactions facade.
 */
public interface IgniteTransactions {
    /**
     * Begins a transaction.
     *
     * @return The future.
     */
    CompletableFuture<Transaction> beginAsync();

    /**
     * Synchronously executes a closure within a transaction.
     * <p>
     * If the closure is executed normally (no exceptions), the transaction is automatically committed.
     *
     * @param clo The closure.
     */
    void runInTransaction(Consumer<Transaction> clo);
}
