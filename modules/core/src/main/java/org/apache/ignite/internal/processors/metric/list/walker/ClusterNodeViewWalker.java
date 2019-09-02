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
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.ClusterNodeView;

/** */
public class ClusterNodeViewWalker implements MonitoringRowAttributeWalker<ClusterNodeView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "id", UUID.class);
        v.accept(1, "consistentId", String.class);
        v.accept(2, "addresses", String.class);
        v.accept(3, "hostNames", String.class);
        v.acceptLong(4, "order");
        v.acceptBoolean(5, "isClient");
        v.acceptBoolean(6, "isDaemon");
        v.acceptBoolean(7, "isLocal");
        v.accept(8, "monitoringRowId", UUID.class);
        v.accept(9, "monitoringRowId", Object.class);
        v.accept(10, "version", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ClusterNodeView row, AttributeWithValueVisitor v) {
        v.accept(0, "id", UUID.class, row.id());
        v.accept(1, "consistentId", String.class, row.consistentId());
        v.accept(2, "addresses", String.class, row.addresses());
        v.accept(3, "hostNames", String.class, row.hostNames());
        v.acceptLong(4, "order", row.order());
        v.acceptBoolean(5, "isClient", row.isClient());
        v.acceptBoolean(6, "isDaemon", row.isDaemon());
        v.acceptBoolean(7, "isLocal", row.isLocal());
        v.accept(8, "monitoringRowId", UUID.class, row.monitoringRowId());
        v.accept(9, "monitoringRowId", Object.class, row.monitoringRowId());
        v.accept(10, "version", String.class, row.version());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 11;
    }
}

