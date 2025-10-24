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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * List of transaction locks for particular key.
 */
public class TxLockList implements Message {
    /** Tx locks. */
    @GridToStringInclude
    @Order(value = 0, method = "transactionLocks")
    private List<TxLock> txLocks = new ArrayList<>();

    /**
     * Default constructor.
     */
    public TxLockList() {
        // No-op.
    }

    /**
     * @return Lock list.
     */
    public List<TxLock> transactionLocks() {
        return txLocks;
    }

    /**
     * @param txLocks Lock list.
     */
    public void transactionLocks(List<TxLock> txLocks) {
        this.txLocks = txLocks;
    }

    /**
     * @param txLock Tx lock.
     */
    public void add(TxLock txLock) {
        txLocks.add(txLock);
    }

    /**
     * @return {@code True} if lock list is empty.
     */
    public boolean isEmpty() {
        return txLocks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLockList.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -26;
    }
}
