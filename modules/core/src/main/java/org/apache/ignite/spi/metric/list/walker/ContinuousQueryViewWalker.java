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
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.ContinuousQueryView;

/** */
public class ContinuousQueryViewWalker implements MonitoringRowAttributeWalker<ContinuousQueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheName", String.class);
        v.accept(1, "nodeId", UUID.class);
        v.accept(2, "topic", String.class);
        v.acceptInt(3, "bufferSize");
        v.acceptLong(4, "interval");
        v.acceptBoolean(5, "autoUnsubscribe");
        v.acceptBoolean(6, "isEvents");
        v.acceptBoolean(7, "isMessaging");
        v.acceptBoolean(8, "isQuery");
        v.acceptBoolean(9, "keepBinary");
        v.acceptBoolean(10, "notifyExisting");
        v.acceptBoolean(11, "oldValueRequired");
        v.acceptLong(12, "lastSendTime");
        v.acceptBoolean(13, "delayedRegister");
        v.accept(14, "localListener", String.class);
        v.accept(15, "remoteFilter", String.class);
        v.accept(16, "remoteTransformer", String.class);
        v.accept(17, "localTransformedListener", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(ContinuousQueryView row, AttributeWithValueVisitor v) {
        v.accept(0, "cacheName", String.class, row.cacheName());
        v.accept(1, "nodeId", UUID.class, row.nodeId());
        v.accept(2, "topic", String.class, row.topic());
        v.acceptInt(3, "bufferSize", row.bufferSize());
        v.acceptLong(4, "interval", row.interval());
        v.acceptBoolean(5, "autoUnsubscribe", row.autoUnsubscribe());
        v.acceptBoolean(6, "isEvents", row.isEvents());
        v.acceptBoolean(7, "isMessaging", row.isMessaging());
        v.acceptBoolean(8, "isQuery", row.isQuery());
        v.acceptBoolean(9, "keepBinary", row.keepBinary());
        v.acceptBoolean(10, "notifyExisting", row.notifyExisting());
        v.acceptBoolean(11, "oldValueRequired", row.oldValueRequired());
        v.acceptLong(12, "lastSendTime", row.lastSendTime());
        v.acceptBoolean(13, "delayedRegister", row.delayedRegister());
        v.accept(14, "localListener", String.class, row.localListener());
        v.accept(15, "remoteFilter", String.class, row.remoteFilter());
        v.accept(16, "remoteTransformer", String.class, row.remoteTransformer());
        v.accept(17, "localTransformedListener", String.class, row.localTransformedListener());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 18;
    }
}

