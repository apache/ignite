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

import org.apache.ignite.internal.DefaultMapperEnumFieldsMessage;
import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord.PartitionState;
import org.apache.ignite.plugin.extensions.communication.CollectionImplementationType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageEnumType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageMapType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.mappers.DefaultEnumMapper;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class DefaultMapperEnumFieldsMessageSerializer implements MessageSerializer<DefaultMapperEnumFieldsMessage> {
    /** */
    private static final GridCacheOperation[] gridCacheOperationVals = GridCacheOperation.values();
    /** */
    private static final PartitionState[] partitionStateVals = PartitionState.values();
    /** */
    private static final TransactionIsolation[] transactionIsolationVals = TransactionIsolation.values();
    /** */
    private static final MessageCollectionType partStatesCollDesc = new MessageCollectionType(new MessageEnumType<>(PartitionState.class, DefaultEnumMapper.INSTANCE::encode, b -> DefaultEnumMapper.INSTANCE.decode(partitionStateVals, b)), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageMapType isolationStringMapCollDesc = new MessageMapType(new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, DefaultEnumMapper.INSTANCE::encode, b -> DefaultEnumMapper.INSTANCE.decode(transactionIsolationVals, b)), CollectionImplementationType.ARRAY_LIST), new MessageItemType(MessageCollectionItemType.STRING), false);

    /** */
    @Override public final boolean writeTo(DefaultMapperEnumFieldsMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByte(DefaultEnumMapper.INSTANCE.encode(msg.publicEnum)))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte(DefaultEnumMapper.INSTANCE.encode(msg.internalEnum)))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(msg.isolationStringMap, isolationStringMapCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(msg.partStates, partStatesCollDesc, ctx))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(DefaultMapperEnumFieldsMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.publicEnum = DefaultEnumMapper.INSTANCE.decode(transactionIsolationVals, reader.readByte());

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.internalEnum = DefaultEnumMapper.INSTANCE.decode(gridCacheOperationVals, reader.readByte());

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.isolationStringMap = reader.readMap(isolationStringMapCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.partStates = reader.readCollection(partStatesCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final DefaultMapperEnumFieldsMessage createMessage() {
        return new DefaultMapperEnumFieldsMessage();
    }
}
