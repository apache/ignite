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

package org.apache.ignite.internal.processors.metric.list.view;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/** */
public class TransactionView implements MonitoringRow<IgniteUuid> {
    /** */
    private IgniteInternalTx tx;

    /**
     * @param tx Transaction.
     */
    public TransactionView(IgniteInternalTx tx) {
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return tx.nodeId().toString();
    }

    /** */
    public long threadId() {
        return tx.threadId();
    }

    /** */
    public long startTime() {
        return tx.startTime();
    }

    /** */
    public TransactionIsolation isolation() {
        return tx.isolation();
    }

    /** */
    public TransactionConcurrency concurrency() {
        return tx.concurrency();
    }

    /** */
    public TransactionState state() {
        return tx.state();
    }

    /** */
    public long timeout() {
        return tx.timeout();
    }

    /** */
    public boolean implicit() {
        return tx.implicit();
    }

    /** */
    public IgniteUuid xid() {
        return tx.xid();
    }

    /** */
    public boolean system() {
        return tx.system();
    }

    /** */
    public boolean implicitSingle() {
        return tx.implicitSingle();
    }

    /** */
    public boolean near() {
        return tx.near();
    }

    /** */
    public boolean dht() {
        return tx.dht();
    }

    /** */
    public boolean colocated() {
        return tx.colocated();
    }

    /** */
    public boolean local() {
        return tx.local();
    }

    /** */
    public UUID subjectId() {
        return tx.subjectId();
    }

    /** */
    public String label() {
        return tx.label();
    }

    /** */
    public boolean onePhaseCommit() {
        return tx.onePhaseCommit();
    }

    /** */
    public boolean internal() {
        return tx.internal();
    }
}
