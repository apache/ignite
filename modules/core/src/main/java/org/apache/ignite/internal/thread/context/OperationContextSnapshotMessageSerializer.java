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

import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.RawField;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry.RU_AWARE_DISTRIBUTED_OP_CTX_FEATURE;
import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.attributesCount;
import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.contains;
import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.nextAttributeId;
import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.set;

/** */
public final class OperationContextSnapshotMessageSerializer implements MessageSerializer<OperationContextSnapshotMessage> {
    /** */
    private static final MessageArrayType ATTRS_TYPE = new MessageArrayType(
        new MessageItemType(MessageCollectionItemType.MSG),
        Message.class
    );

    /** */
    private final DistributedOperationContext distrOpCtx;

    /** */
    public OperationContextSnapshotMessageSerializer(GridKernalContext ctx) {
        distrOpCtx = ctx.distributedOperationContext();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(OperationContextSnapshotMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        byte idBitmap = attributesToInclude(msg.idBitmap, ctx);

        Message[] attrs = idBitmap == msg.idBitmap ? msg.attrs : valuesToInclude(msg, idBitmap);

        return isCompactSerializationSupported(ctx)
            ? doWrite(msg, idBitmap, attrs, writer, ctx)
            : doWriteLegacy(msg, idBitmap, attrs, writer, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        return isCompactSerializationSupported(ctx) ? doRead(msg, reader, ctx) : doReadLegacy(msg, reader, ctx);
    }

    /** {@inheritDoc} */
    @Override public OperationContextSnapshotMessage createMessage() {
        return new OperationContextSnapshotMessage();
    }

    /** */
    private boolean doWrite(
        OperationContextSnapshotMessage msg,
        byte idBitmap,
        Message[] attrs,
        MessageWriter writer,
        MessageSerializationContext ctx
    ) {
        if (writer.state() == 0) {
            if (!writer.writeByte(idBitmap))
                return false;

            addPendingRawAttributes(msg, idBitmap, writer, ctx);

            writer.incrementState();
        }

        for (int valIdx = writer.state() - 1; valIdx < attrs.length; valIdx = writer.state() - 1) {
            if (!writer.writeMessage(attrs[valIdx], ctx))
                return false;

            writer.incrementState();
        }

        return writeRawFields(msg, writer, ctx);
    }

    /** */
    private boolean doRead(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
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

        return readRawFields(msg, reader, ctx);
    }

    /** */
    private boolean doWriteLegacy(
        OperationContextSnapshotMessage msg,
        byte idBitmap,
        Message[] attrs,
        MessageWriter writer,
        MessageSerializationContext ctx
    ) {
        switch (writer.state()) {
            case 0:
                if (!writer.writeObjectArray(attrs, ATTRS_TYPE, ctx))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte(idBitmap))
                    return false;

                addPendingRawAttributes(msg, idBitmap, writer, ctx);

                writer.incrementState();

            case 2:
                return writeRawFields(msg, writer, ctx);
        }

        return true;
    }

    /** */
    private boolean doReadLegacy(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
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

            case 2:
                return readRawFields(msg, reader, ctx);
        }

        return true;
    }

    /** */
    private boolean writeRawFields(OperationContextSnapshotMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (ctx.includeRawFields()) {
            if (!writer.writeRawFields(msg.rawFields()))
                return false;
        }

        writer.incrementState();

        return true;
    }

    /** */
    private boolean readRawFields(OperationContextSnapshotMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        if (ctx.includeRawFields()) {
            List<RawField> rawFields = reader.readRawFields();

            if (!reader.isLastRead())
                return false;

            rawFields.removeIf(f -> consumeKnownAttribute(msg, f, reader, ctx));

            if (!rawFields.isEmpty())
                msg.rawFields(rawFields);
        }

        reader.incrementState();

        return true;
    }

    /** */
    private boolean consumeKnownAttribute(
        OperationContextSnapshotMessage msg,
        RawField f,
        MessageReader reader,
        MessageSerializationContext ctx
    ) {
        int attrId = f.tag();

        if (distrOpCtx.key(attrId) == null || contains(msg.idBitmap, attrId))
            return false;

        msg.setValue(attrId, reader.deserializeRawField(f, in -> in.readMessage(ctx)));

        return true;
    }

    /** */
    private static void addPendingRawAttributes(
        OperationContextSnapshotMessage msg,
        byte includedIds,
        MessageWriter writer,
        MessageSerializationContext ctx
    ) {
        if (includedIds == msg.idBitmap || !ctx.includeRawFields())
            return;

        byte excludedIds = (byte)(msg.idBitmap & ~includedIds);

        for (int attrId = nextAttributeId(excludedIds, -1); attrId >= 0; attrId = nextAttributeId(excludedIds, attrId)) {
            Message val = msg.getValue(attrId);

            writer.addPendingRawField(attrId, w -> w.writeMessage(val, ctx));
        }
    }

    /** */
    private byte attributesToInclude(byte idBitmap, MessageSerializationContext ctx) {
        byte res = 0;

        for (int attrId = nextAttributeId(idBitmap, -1); attrId >= 0; attrId = nextAttributeId(idBitmap, attrId)) {
            DistributedAttributeKey key = distrOpCtx.key(attrId);

            assert key != null : "Distributed attribute is not registered [id=" + attrId + ']';

            if (key.introducedBy() == null || ctx.includeFieldIntroducedBy(key.introducedBy()))
                res = set(res, attrId);
        }

        return res;
    }

    /** */
    private static Message[] valuesToInclude(OperationContextSnapshotMessage msg, byte includedIds) {
        Message[] res = new Message[attributesCount(includedIds)];

        int resIdx = 0;

        for (int attrId = nextAttributeId(includedIds, -1); attrId >= 0; attrId = nextAttributeId(includedIds, attrId))
            res[resIdx++] = msg.getValue(attrId);

        return res;
    }

    /** */
    private static boolean isCompactSerializationSupported(MessageSerializationContext ctx) {
        return ctx.includeFieldIntroducedBy(RU_AWARE_DISTRIBUTED_OP_CTX_FEATURE);
    }
}
