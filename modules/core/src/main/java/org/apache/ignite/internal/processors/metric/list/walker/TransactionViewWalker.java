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
        v.acceptBoolean(0, "colocated");
        v.accept(1, "concurrency", TransactionConcurrency.class);
        v.acceptBoolean(2, "dht");
        v.acceptBoolean(3, "implicit");
        v.acceptBoolean(4, "implicitSingle");
        v.acceptBoolean(5, "internal");
        v.accept(6, "isolation", TransactionIsolation.class);
        v.accept(7, "label", String.class);
        v.acceptBoolean(8, "local");
        v.acceptBoolean(9, "near");
        v.accept(10, "nodeId", UUID.class);
        v.acceptBoolean(11, "onePhaseCommit");
        v.acceptLong(12, "startTime");
        v.accept(13, "state", TransactionState.class);
        v.accept(14, "subjectId", UUID.class);
        v.acceptBoolean(15, "system");
        v.acceptLong(16, "threadId");
        v.acceptLong(17, "timeout");
        v.accept(18, "xid", IgniteUuid.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(TransactionView row, AttributeWithValueVisitor v) {
        v.acceptBoolean(0, "colocated", row.colocated());
        v.accept(1, "concurrency", TransactionConcurrency.class, row.concurrency());
        v.acceptBoolean(2, "dht", row.dht());
        v.acceptBoolean(3, "implicit", row.implicit());
        v.acceptBoolean(4, "implicitSingle", row.implicitSingle());
        v.acceptBoolean(5, "internal", row.internal());
        v.accept(6, "isolation", TransactionIsolation.class, row.isolation());
        v.accept(7, "label", String.class, row.label());
        v.acceptBoolean(8, "local", row.local());
        v.acceptBoolean(9, "near", row.near());
        v.accept(10, "nodeId", UUID.class, row.nodeId());
        v.acceptBoolean(11, "onePhaseCommit", row.onePhaseCommit());
        v.acceptLong(12, "startTime", row.startTime());
        v.accept(13, "state", TransactionState.class, row.state());
        v.accept(14, "subjectId", UUID.class, row.subjectId());
        v.acceptBoolean(15, "system", row.system());
        v.acceptLong(16, "threadId", row.threadId());
        v.acceptLong(17, "timeout", row.timeout());
        v.accept(18, "xid", IgniteUuid.class, row.xid());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 19;
    }
}

