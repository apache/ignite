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
import org.apache.ignite.spi.metric.list.view.ContinuousQueryView;

/** */
public class ContinuousQueryViewWalker implements MonitoringRowAttributeWalker<ContinuousQueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.acceptBoolean(0, "autoUnsubscribe");
        v.acceptInt(1, "bufferSize");
        v.accept(2, "cacheName", String.class);
        v.acceptBoolean(3, "delayedRegister");
        v.acceptLong(4, "interval");
        v.acceptBoolean(5, "isEvents");
        v.acceptBoolean(6, "isMessaging");
        v.acceptBoolean(7, "isQuery");
        v.acceptBoolean(8, "keepBinary");
        v.acceptLong(9, "lastSendTime");
        v.accept(10, "localListener", String.class);
        v.accept(11, "localTransformedListener", String.class);
        v.accept(12, "nodeId", UUID.class);
        v.acceptBoolean(13, "notifyExisting");
        v.acceptBoolean(14, "oldValueRequired");
        v.accept(15, "remoteFilter", String.class);
        v.accept(16, "remoteTransformer", String.class);
        v.accept(17, "topic", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ContinuousQueryView row, AttributeWithValueVisitor v) {
        v.acceptBoolean(0, "autoUnsubscribe", row.autoUnsubscribe());
        v.acceptInt(1, "bufferSize", row.bufferSize());
        v.accept(2, "cacheName", String.class, row.cacheName());
        v.acceptBoolean(3, "delayedRegister", row.delayedRegister());
        v.acceptLong(4, "interval", row.interval());
        v.acceptBoolean(5, "isEvents", row.isEvents());
        v.acceptBoolean(6, "isMessaging", row.isMessaging());
        v.acceptBoolean(7, "isQuery", row.isQuery());
        v.acceptBoolean(8, "keepBinary", row.keepBinary());
        v.acceptLong(9, "lastSendTime", row.lastSendTime());
        v.accept(10, "localListener", String.class, row.localListener());
        v.accept(11, "localTransformedListener", String.class, row.localTransformedListener());
        v.accept(12, "nodeId", UUID.class, row.nodeId());
        v.acceptBoolean(13, "notifyExisting", row.notifyExisting());
        v.acceptBoolean(14, "oldValueRequired", row.oldValueRequired());
        v.accept(15, "remoteFilter", String.class, row.remoteFilter());
        v.accept(16, "remoteTransformer", String.class, row.remoteTransformer());
        v.accept(17, "topic", String.class, row.topic());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 18;
    }
}

