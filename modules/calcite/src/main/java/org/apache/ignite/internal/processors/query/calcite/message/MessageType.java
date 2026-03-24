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
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroupSerializer;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescriptionSerializer;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;

/**
 *
 */
public enum MessageType {
    /** */
    QUERY_START_REQUEST(QueryStartRequest::new, new QueryStartRequestSerializer(300)),

    /** */
    QUERY_START_RESPONSE(QueryStartResponse::new, new QueryStartResponseSerializer(301)),

    /** */
    QUERY_ERROR_MESSAGE(CalciteErrorMessage::new, new CalciteErrorMessageSerializer(302)),

    /** */
    QUERY_BATCH_MESSAGE(QueryBatchMessage::new, new QueryBatchMessageSerializer(303)),

    /** */
    QUERY_ACKNOWLEDGE_MESSAGE(QueryBatchAcknowledgeMessage::new, new QueryBatchAcknowledgeMessageSerializer(304)),

    /** */
    QUERY_INBOX_CANCEL_MESSAGE(InboxCloseMessage::new, new InboxCloseMessageSerializer(305)),

    /** */
    QUERY_CLOSE_MESSAGE(QueryCloseMessage::new, new QueryCloseMessageSerializer(306)),

    /** */
    GENERIC_VALUE_MESSAGE(GenericValueMessage::new, new GenericValueMessageSerializer(307)),

    /** */
    FRAGMENT_MAPPING(FragmentMapping::new, new FragmentMappingSerializer(350)),

    /** */
    COLOCATION_GROUP(ColocationGroup::new, new ColocationGroupSerializer(351)),

    /** */
    FRAGMENT_DESCRIPTION(FragmentDescription::new, new FragmentDescriptionSerializer(352)),

    /** */
    QUERY_TX_ENTRY(QueryTxEntry::new, new QueryTxEntrySerializer(353));

    /** */
    private final Supplier<CalciteMessage> factory;

    /** */
    private final MessageSerializer serializer;

    /**
     * @param factory Message factory.
     * @param serializer Message serializer.
     */
    MessageType(Supplier<CalciteMessage> factory, MessageSerializer serializer) {
        this.factory = factory;
        this.serializer = serializer;
    }

    /**
     * @return Message factory.
     */
    public Supplier<CalciteMessage> factory() {
        return factory;
    }

    /**
     * @return Message direct type.
     */
    public short directType() {
        return serializer.directType();
    }

    /**
     * @return Message serializer.
     */
    public MessageSerializer serializer() {
        return serializer;
    }
}
