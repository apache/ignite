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
import org.apache.ignite.spi.metric.list.ServiceView;

/** */
public class ServiceViewWalker implements MonitoringRowAttributeWalker<ServiceView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "nodeFilter", Class.class);
        v.accept(1, "cacheName", String.class);
        v.accept(2, "serviceClass", Class.class);
        v.acceptInt(3, "totalCount");
        v.acceptInt(4, "maxPerNodeCount");
        v.accept(5, "affinityKeyValue", String.class);
        v.acceptBoolean(6, "staticallyConfigured");
        v.accept(7, "originNodeId", UUID.class);
        v.accept(8, "name", String.class);
        v.accept(9, "id", IgniteUuid.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ServiceView row, AttributeWithValueVisitor v) {
        v.accept(0, "nodeFilter", Class.class, row.nodeFilter());
        v.accept(1, "cacheName", String.class, row.cacheName());
        v.accept(2, "serviceClass", Class.class, row.serviceClass());
        v.acceptInt(3, "totalCount", row.totalCount());
        v.acceptInt(4, "maxPerNodeCount", row.maxPerNodeCount());
        v.accept(5, "affinityKeyValue", String.class, row.affinityKeyValue());
        v.acceptBoolean(6, "staticallyConfigured", row.staticallyConfigured());
        v.accept(7, "originNodeId", UUID.class, row.originNodeId());
        v.accept(8, "name", String.class, row.name());
        v.accept(9, "id", IgniteUuid.class, row.id());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 10;
    }
}

