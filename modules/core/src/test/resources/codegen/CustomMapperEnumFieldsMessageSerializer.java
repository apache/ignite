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

package org.apache.ignite.internal.codegen;

import org.apache.ignite.internal.CustomMapperEnumFieldsMessage;
import org.apache.ignite.internal.TransactionIsolationEnumMapper;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.mappers.EnumMapper;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class CustomMapperEnumFieldsMessageSerializer implements MessageSerializer {
    /** */
    private final EnumMapper<TransactionIsolation> transactionIsolationMapper = new TransactionIsolationEnumMapper();

    /** */
    @Override public boolean writeTo(Message m, MessageWriter writer) {
        CustomMapperEnumFieldsMessage msg = (CustomMapperEnumFieldsMessage)m;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByte(transactionIsolationMapper.encode(msg.txMode())))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, MessageReader reader) {
        CustomMapperEnumFieldsMessage msg = (CustomMapperEnumFieldsMessage)m;

        switch (reader.state()) {
            case 0:
                msg.txMode(transactionIsolationMapper.decode(reader.readByte()));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}