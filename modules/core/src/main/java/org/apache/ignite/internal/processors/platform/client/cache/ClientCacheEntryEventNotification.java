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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientNotification;

import javax.cache.event.CacheEntryEvent;

/**
 * Continuous query notification.
 */
@SuppressWarnings("rawtypes")
public class ClientCacheEntryEventNotification extends ClientNotification {
    /** */
    private final CacheEntryEvent evt;

    /**
     * Ctor.
     * @param opCode Operation code.
     * @param rsrcId Resource ID.
     */
    public ClientCacheEntryEventNotification(short opCode, long rsrcId, CacheEntryEvent evt) {
        super(opCode, rsrcId);

        assert evt != null;
        this.evt = evt;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeObjectDetached(evt.getKey());
        writer.writeObjectDetached(evt.getOldValue());
        writer.writeObjectDetached(evt.getValue());

        switch (evt.getEventType()) {
            case CREATED: writer.writeByte((byte) 0); break;
            case UPDATED: writer.writeByte((byte) 1); break;
            case REMOVED: writer.writeByte((byte) 2); break;
            case EXPIRED: writer.writeByte((byte) 3); break;
            default:
                throw new IllegalArgumentException("Unknown event type: " + evt.getEventType());
        }
    }
}
