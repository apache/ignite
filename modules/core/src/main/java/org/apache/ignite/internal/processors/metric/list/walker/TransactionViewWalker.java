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

package org.apache.ignite.internal.processors.metric.list.walker;

import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.TransactionView;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/** */
public class TransactionViewWalker implements MonitoringRowAttributeWalker<TransactionView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "nodeId", UUID.class);
        v.accept(1, "state", TransactionState.class);
        v.accept(2, "xid", IgniteUuid.class);
        v.accept(3, "label", String.class);
        v.acceptLong(4, "startTime");
        v.accept(5, "isolation", TransactionIsolation.class);
        v.accept(6, "concurrency", TransactionConcurrency.class);
        v.acceptBoolean(7, "colocated");
        v.acceptBoolean(8, "dht");
        v.acceptBoolean(9, "implicit");
        v.acceptBoolean(10, "implicitSingle");
        v.acceptBoolean(11, "internal");
        v.acceptBoolean(12, "local");
        v.acceptBoolean(13, "near");
        v.acceptBoolean(14, "onePhaseCommit");
        v.accept(15, "subjectId", UUID.class);
        v.acceptBoolean(16, "system");
        v.acceptLong(17, "threadId");
        v.acceptLong(18, "timeout");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(TransactionView row, AttributeWithValueVisitor v) {
        v.accept(0, "nodeId", UUID.class, row.nodeId());
        v.accept(1, "state", TransactionState.class, row.state());
        v.accept(2, "xid", IgniteUuid.class, row.xid());
        v.accept(3, "label", String.class, row.label());
        v.acceptLong(4, "startTime", row.startTime());
        v.accept(5, "isolation", TransactionIsolation.class, row.isolation());
        v.accept(6, "concurrency", TransactionConcurrency.class, row.concurrency());
        v.acceptBoolean(7, "colocated", row.colocated());
        v.acceptBoolean(8, "dht", row.dht());
        v.acceptBoolean(9, "implicit", row.implicit());
        v.acceptBoolean(10, "implicitSingle", row.implicitSingle());
        v.acceptBoolean(11, "internal", row.internal());
        v.acceptBoolean(12, "local", row.local());
        v.acceptBoolean(13, "near", row.near());
        v.acceptBoolean(14, "onePhaseCommit", row.onePhaseCommit());
        v.accept(15, "subjectId", UUID.class, row.subjectId());
        v.acceptBoolean(16, "system", row.system());
        v.acceptLong(17, "threadId", row.threadId());
        v.acceptLong(18, "timeout", row.timeout());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 19;
    }
}

