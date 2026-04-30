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
import org.apache.ignite.internal.KeyCacheObjectEntryMsgSerializer;
import org.apache.ignite.internal.TestKeyCacheObjectCollectionMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestKeyCacheObjectCollectionMessageSerializer implements MessageSerializer<TestKeyCacheObjectCollectionMessage> {
    /** */
    private final static KeyCacheObjectEntryMsgSerializer KEY_CACHE_OBJECT_ENTRY_MSG_SER = new KeyCacheObjectEntryMsgSerializer();
    /** */
    private static final MessageArrayType entriesArrCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.MSG), KeyCacheObjectEntryMsg.class);
    /** */
    private static final MessageCollectionType entriesCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), false);

    /** */
    @Override public boolean writeTo(TestKeyCacheObjectCollectionMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.entries, entriesCollDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeObjectArray(msg.entriesArr, entriesArrCollDesc))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestKeyCacheObjectCollectionMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.entries = reader.readCollection(entriesCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.entriesArr = reader.readObjectArray(entriesArrCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** */
    @Override public void prepareMarshalCacheObjects(TestKeyCacheObjectCollectionMessage msg, CacheObjectValueContext ctx, GridCacheSharedContext sharedCtx) throws IgniteCheckedException {
        if (msg.entries != null) {
            for (KeyCacheObjectEntryMsg e : msg.entries) {
                if (e != null)
                    KEY_CACHE_OBJECT_ENTRY_MSG_SER.prepareMarshalCacheObjects(e, ctx, sharedCtx);
            }
        }

        if (msg.entriesArr != null) {
            for (KeyCacheObjectEntryMsg e : msg.entriesArr) {
                if (e != null)
                    KEY_CACHE_OBJECT_ENTRY_MSG_SER.prepareMarshalCacheObjects(e, ctx, sharedCtx);
            }
        }
    }
}
