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

package org.apache.ignite.internal.thread.context;

import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry.RU_AWARE_DISTRIBUTED_ATTRIBUTE_FEATURE;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.attributesCount;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.contains;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.set;

/** */
public final class OperationContextSnapshotMessageSerializer implements MessageSerializer<OperationContextSnapshotMessage> {
    /** */
    private static final MessageArrayType ATTRS_TYPE = new MessageArrayType(
        new MessageItemType(MessageCollectionItemType.MSG),
        Message.class
    );

    /** {@inheritDoc} */
    @Override public boolean writeTo(OperationContextSnapshotMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        byte idBitmap = filterIds(msg.idBitmap, ctx);

        Message[] attrs = idBitmap == msg.idBitmap ? msg.attrs : filterValues(msg, idBitmap);

        return isCompactSerializationSupported(ctx)
            ? writeCompact(idBitmap, attrs, writer, ctx)
            : writeLegacy(idBitmap, attrs, writer, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        return isCompactSerializationSupported(ctx) ? readCompact(msg, reader, ctx) : readLegacy(msg, reader, ctx);
    }

    /** {@inheritDoc} */
    @Override public OperationContextSnapshotMessage createMessage() {
        return new OperationContextSnapshotMessage();
    }

    /** */
    private static boolean writeCompact(byte idBitmap, Message[] attrs, MessageWriter writer, MessageSerializationContext ctx) {
        if (writer.state() == 0) {
            if (!writer.writeByte(idBitmap))
                return false;

            writer.incrementState();
        }

        for (int valIdx = writer.state() - 1; valIdx < attrs.length; valIdx = writer.state() - 1) {
            if (!writer.writeMessage(attrs[valIdx], ctx))
                return false;

            writer.incrementState();
        }

        return true;
    }

    /** */
    private static boolean readCompact(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        if (reader.state() == 0) {
            msg.idBitmap = reader.readByte();

            if (!reader.isLastRead())
                return false;

            msg.attrs = new Message[attributesCount(msg.idBitmap)];

            reader.incrementState();
        }

        for (int valIdx = reader.state() - 1; valIdx < msg.attrs.length; valIdx = reader.state() - 1) {
            msg.attrs[valIdx] = reader.readMessage(ctx);

            if (!reader.isLastRead())
                return false;

            reader.incrementState();
        }

        return true;
    }

    /** */
    private static boolean writeLegacy(byte idBitmap, Message[] attrs, MessageWriter writer, MessageSerializationContext ctx) {
        switch (writer.state()) {
            case 0:
                if (!writer.writeObjectArray(attrs, ATTRS_TYPE, ctx))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte(idBitmap))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    private static boolean readLegacy(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.attrs = reader.readObjectArray(ATTRS_TYPE, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.idBitmap = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** */
    private static byte filterIds(byte idBitmap, MessageSerializationContext ctx) {
        byte res = 0;

        for (int attrId = 0; attrId < MAX_ATTRS_CNT; attrId++) {
            if (!contains(idBitmap, attrId))
                continue;

            DistributedAttributeKey key = DistributedAttributeKeyRegistry.get(attrId);

            assert key != null;

            if (key.introducedBy() == null || ctx.includeFieldIntroducedBy(key.introducedBy()))
                res = set(res, attrId);
        }

        return res;
    }

    /** */
    private static Message[] filterValues(OperationContextSnapshotMessage msg, byte includedIds) {
        Message[] res = new Message[attributesCount(includedIds)];

        for (int attrId = 0, valIdx = 0, resIdx = 0; attrId < MAX_ATTRS_CNT && resIdx < res.length; attrId++) {
            if (!contains(msg.idBitmap, attrId))
                continue;

            if (contains(includedIds, attrId))
                res[resIdx++] = msg.attrs[valIdx];

            valIdx++;
        }

        return res;
    }

    /** */
    private static boolean isCompactSerializationSupported(MessageSerializationContext ctx) {
        return ctx.includeFieldIntroducedBy(RU_AWARE_DISTRIBUTED_ATTRIBUTE_FEATURE);
    }
}
