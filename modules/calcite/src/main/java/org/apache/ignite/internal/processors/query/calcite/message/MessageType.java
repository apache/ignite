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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.FragmentDescription;

/**
 *
 */
public enum MessageType {
    QUERY_START_REQUEST(300, QueryStartRequest::new),
    QUERY_START_RESPONSE(301, QueryStartResponse::new),
    QUERY_CANCEL_REQUEST(302, QueryCancelRequest::new),
    QUERY_BATCH_MESSAGE(303, QueryBatchMessage::new),
    QUERY_ACKNOWLEDGE_MESSAGE(304, QueryBatchAcknowledgeMessage::new),
    QUERY_INBOX_CANCEL_MESSAGE(305, InboxCancelMessage::new),
    GENERIC_ROW_MESSAGE(306, GenericRowMessage::new),
    NODES_MAPPING(350, NodesMapping::new),
    FRAGMENT_DESCRIPTION(351, FragmentDescription::new);

    /** */
    private final int directType;

    /** */
    private final Supplier<CalciteMessage> factory;

    /**
     * @param directType Message direct type.
     */
    MessageType(int directType, Supplier<CalciteMessage> factory) {
        this.directType = directType;
        this.factory = factory;
    }

    /**
     * @return Message direct type;
     */
    public short directType() {
        return (short) directType;
    }

    /** */
    private CalciteMessage newMessage() {
        CalciteMessage msg = factory.get();

        assert msg.type() == this;

        return msg;
    }

    /**
     * Message factory method.
     *
     * @param directType Message direct type.
     * @return new message or {@code null} in case of unknown message direct type.
     */
    public static CalciteMessage newMessage(short directType) {
        switch (directType) {
            case 300:
                return QUERY_START_REQUEST.newMessage();
            case 301:
                return QUERY_START_RESPONSE.newMessage();
            case 302:
                return QUERY_CANCEL_REQUEST.newMessage();
            case 303:
                return QUERY_BATCH_MESSAGE.newMessage();
            case 304:
                return QUERY_ACKNOWLEDGE_MESSAGE.newMessage();
            case 305:
                return QUERY_INBOX_CANCEL_MESSAGE.newMessage();
            case 306:
                return GENERIC_ROW_MESSAGE.newMessage();
            case 350:
                return NODES_MAPPING.newMessage();
            case 351:
                return FRAGMENT_DESCRIPTION.newMessage();
            default:
                return null;
        }
    }
}
