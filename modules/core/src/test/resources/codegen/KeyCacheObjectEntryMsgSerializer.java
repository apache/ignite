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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.KeyCacheObjectEntryMsg;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class KeyCacheObjectEntryMsgSerializer implements MessageSerializer<KeyCacheObjectEntryMsg> {
    /** */
    private final static GridCacheVersionSerializer GRID_CACHE_VERSION_SER = new GridCacheVersionSerializer();

    /** */
    @Override public boolean writeTo(KeyCacheObjectEntryMsg msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeKeyCacheObject(msg.key))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage(msg.val))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(KeyCacheObjectEntryMsg msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.key = reader.readKeyCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.val = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** */
    @Override public void prepareMarshalCacheObjects(KeyCacheObjectEntryMsg msg, CacheObjectValueContext ctx, GridCacheSharedContext sharedCtx) throws IgniteCheckedException {
        if (msg.key != null)
            msg.key.prepareMarshal(ctx);

        if (msg.val != null)
            GRID_CACHE_VERSION_SER.prepareMarshalCacheObjects(msg.val, ctx, sharedCtx);
    }
}