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

import java.util.List;
import org.apache.ignite.internal.CustomMapperEnumSetMessage;
import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.internal.TransactionIsolationEnumMapper;
import org.apache.ignite.plugin.extensions.communication.AbstractMessage;
import org.apache.ignite.plugin.extensions.communication.CollectionImplementationType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageEnumType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.RawField;
import org.apache.ignite.plugin.extensions.communication.mappers.EnumMapper;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class CustomMapperEnumSetMessageSerializer implements MessageSerializer<CustomMapperEnumSetMessage> {
    /** */
    private static final EnumMapper<TransactionIsolation> transactionIsolationMapper = new TransactionIsolationEnumMapper();
    /** */
    private static final MessageCollectionType isolationSetCollDesc = new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, transactionIsolationMapper::encode, transactionIsolationMapper::decode), CollectionImplementationType.ENUM_SET);
    /** */
    private static final MessageCollectionType isolationsCollDesc = new MessageCollectionType(new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, transactionIsolationMapper::encode, transactionIsolationMapper::decode), CollectionImplementationType.ENUM_SET), CollectionImplementationType.ARRAY_LIST);

    /** */
    @Override public final boolean writeTo(CustomMapperEnumSetMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.isolationSet, isolationSetCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.isolations, isolationsCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 2:
                if (ctx.includeRawFields()) {
                    if (!writer.writeRawFields(msg.rawFields()))
                        return false;
                }

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(CustomMapperEnumSetMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.isolationSet = reader.readCollection(isolationSetCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.isolations = reader.readCollection(isolationsCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                if (ctx.includeRawFields()) {
                    List<RawField> rawFields = reader.readRawFields();

                    if (!reader.isLastRead())
                        return false;

                    if (!rawFields.isEmpty())
                        msg.rawFields(rawFields);
                }

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final CustomMapperEnumSetMessage createMessage() {
        return new CustomMapperEnumSetMessage();
    }
}
