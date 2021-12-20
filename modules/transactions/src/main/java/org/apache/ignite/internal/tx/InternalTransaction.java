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

package org.apache.ignite.internal.tx;

import java.util.Set;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * An extension of a transaction for internal usage.
 */
public interface InternalTransaction extends Transaction {
    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    @NotNull Timestamp timestamp();

    /**
     * Returns a set of enlisted partition groups.
     *
     * @return A set of enlisted partition groups.
     */
    @TestOnly
    Set<RaftGroupService> enlisted();

    /**
     * Returns a transaction state.
     *
     * @return The state.
     */
    TxState state();

    /**
     * Enlists a partition group into a transaction.
     *
     * @param svc Partition service.
     * @return {@code True} if a partition is enlisted into the transaction.
     */
    boolean enlist(RaftGroupService svc);
}
