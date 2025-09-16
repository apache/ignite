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
import org.apache.ignite.internal.codegen.FragmentDescriptionSerializer;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;

/**
 *
 */
public enum MessageType {
    /** */
    QUERY_START_REQUEST(300, QueryStartRequest::new),

    /** */
    QUERY_START_RESPONSE(301, QueryStartResponse::new),

    /** */
    QUERY_ERROR_MESSAGE(302, ErrorMessage::new),

    /** */
    QUERY_BATCH_MESSAGE(303, QueryBatchMessage::new),

    /** */
    QUERY_ACKNOWLEDGE_MESSAGE(304, QueryBatchAcknowledgeMessage::new),

    /** */
    QUERY_INBOX_CANCEL_MESSAGE(305, InboxCloseMessage::new),

    /** */
    QUERY_CLOSE_MESSAGE(306, QueryCloseMessage::new),

    /** */
    GENERIC_VALUE_MESSAGE(307, GenericValueMessage::new),

    /** */
    FRAGMENT_MAPPING(350, FragmentMapping::new),

    /** */
    COLOCATION_GROUP(351, ColocationGroup::new),

    /** */
    FRAGMENT_DESCRIPTION(352, FragmentDescription::new, new FragmentDescriptionSerializer()),

    /** */
    QUERY_TX_ENTRY(353, QueryTxEntry::new);

    /** */
    private final int directType;

    /** */
    private final Supplier<CalciteMessage> factory;

    /** */
    private MessageSerializer serializer;

    /**
     * @param directType Message direct type.
     * @param factory Message factory.
     */
    MessageType(int directType, Supplier<CalciteMessage> factory) {
        this.directType = directType;
        this.factory = factory;
    }

    /**
     * @param directType Message direct type.
     * @param factory Message factory.
     * @param serializer Message serializer.
     */
    MessageType(int directType, Supplier<CalciteMessage> factory, MessageSerializer serializer) {
        this.directType = directType;
        this.factory = factory;
        this.serializer = serializer;
    }

    /**
     * @return Message direct type;
     */
    public short directType() {
        return (short)directType;
    }

    /**
     * @return Message factory.
     */
    public Supplier<CalciteMessage> factory() {
        return factory;
    }

    /**
     * @return Message serializer.
     */
    public MessageSerializer serializer() {
        return serializer;
    }
}
