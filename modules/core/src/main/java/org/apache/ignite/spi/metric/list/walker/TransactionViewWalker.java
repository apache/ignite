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

package org.apache.ignite.spi.metric.list.walker;

import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.TransactionView;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/** */
public class TransactionViewWalker implements MonitoringRowAttributeWalker<TransactionView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.acceptLong(0, "timeout");
        v.acceptLong(1, "threadId");
        v.accept(2, "nodeId", UUID.class);
        v.acceptLong(3, "startTime");
        v.acceptBoolean(4, "local");
        v.accept(5, "isolation", TransactionIsolation.class);
        v.accept(6, "concurrency", TransactionConcurrency.class);
        v.acceptBoolean(7, "implicit");
        v.accept(8, "xid", IgniteUuid.class);
        v.acceptBoolean(9, "implicitSingle");
        v.acceptBoolean(10, "near");
        v.acceptBoolean(11, "dht");
        v.acceptBoolean(12, "colocated");
        v.accept(13, "subjectId", UUID.class);
        v.accept(14, "label", String.class);
        v.acceptBoolean(15, "onePhaseCommit");
        v.acceptBoolean(16, "internal");
        v.acceptBoolean(17, "system");
        v.accept(18, "state", TransactionState.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(TransactionView row, AttributeWithValueVisitor v) {
        v.acceptLong(0, "timeout", row.timeout());
        v.acceptLong(1, "threadId", row.threadId());
        v.accept(2, "nodeId", UUID.class, row.nodeId());
        v.acceptLong(3, "startTime", row.startTime());
        v.acceptBoolean(4, "local", row.local());
        v.accept(5, "isolation", TransactionIsolation.class, row.isolation());
        v.accept(6, "concurrency", TransactionConcurrency.class, row.concurrency());
        v.acceptBoolean(7, "implicit", row.implicit());
        v.accept(8, "xid", IgniteUuid.class, row.xid());
        v.acceptBoolean(9, "implicitSingle", row.implicitSingle());
        v.acceptBoolean(10, "near", row.near());
        v.acceptBoolean(11, "dht", row.dht());
        v.acceptBoolean(12, "colocated", row.colocated());
        v.accept(13, "subjectId", UUID.class, row.subjectId());
        v.accept(14, "label", String.class, row.label());
        v.acceptBoolean(15, "onePhaseCommit", row.onePhaseCommit());
        v.acceptBoolean(16, "internal", row.internal());
        v.acceptBoolean(17, "system", row.system());
        v.accept(18, "state", TransactionState.class, row.state());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 19;
    }
}

