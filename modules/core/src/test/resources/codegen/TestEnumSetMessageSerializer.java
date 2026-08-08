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

import org.apache.ignite.internal.TestEnumSetMessage;
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
public final class TestEnumSetMessageSerializer implements MessageSerializer<TestEnumSetMessage> {
    /** */
    private static final TransactionIsolation[] transactionIsolationVals = TransactionIsolation.values();
    /** */
    private static final MessageCollectionType isolationsCollDesc = new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, DefaultEnumMapper.INSTANCE::encode, b -> DefaultEnumMapper.INSTANCE.decode(transactionIsolationVals, b)), CollectionImplementationType.ENUM_SET);
    /** */
    private static final MessageCollectionType isolationsListCollDesc = new MessageCollectionType(new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, DefaultEnumMapper.INSTANCE::encode, b -> DefaultEnumMapper.INSTANCE.decode(transactionIsolationVals, b)), CollectionImplementationType.ENUM_SET), CollectionImplementationType.ARRAY_LIST);
    /** */
    private static final MessageMapType isolationsMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.STRING), new MessageCollectionType(new MessageEnumType<>(TransactionIsolation.class, DefaultEnumMapper.INSTANCE::encode, b -> DefaultEnumMapper.INSTANCE.decode(transactionIsolationVals, b)), CollectionImplementationType.ENUM_SET), false);

    /** */
    @Override public final boolean writeTo(TestEnumSetMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.isolations, isolationsCollDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(msg.isolationsMap, isolationsMapCollDesc))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(msg.isolationsList, isolationsListCollDesc))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(TestEnumSetMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.isolations = reader.readCollection(isolationsCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.isolationsMap = reader.readMap(isolationsMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.isolationsList = reader.readCollection(isolationsListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final TestEnumSetMessage createMessage() {
        return new TestEnumSetMessage();
    }
}
