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

import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public enum MessageType {
    /** */
    QUERY_START_REQUEST(300, QueryStartRequest.class),

    /** */
    QUERY_START_RESPONSE(301, QueryStartResponse.class),

    /** */
    QUERY_ERROR_MESSAGE(302, CalciteErrorMessage.class),

    /** */
    QUERY_BATCH_MESSAGE(303, QueryBatchMessage.class),

    /** */
    QUERY_BATCH_ACKNOWLEDGE_MESSAGE(304, QueryBatchAcknowledgeMessage.class),

    /** */
    QUERY_INBOX_CANCEL_MESSAGE(305, QueryInboxCloseMessage.class),

    /** */
    QUERY_CLOSE_MESSAGE(306, QueryCloseMessage.class),

    /** */
    GENERIC_VALUE_MESSAGE(307, GenericValueMessage.class),

    /** */
    FRAGMENT_MAPPING(350, FragmentMapping.class),

    /** */
    COLOCATION_GROUP(351, ColocationGroup.class),

    /** */
    FRAGMENT_DESCRIPTION(352, FragmentDescription.class),

    /** */
    QUERY_TX_ENTRY(353, QueryTxEntry.class);

    /** */
    private final short directType;

    /** */
    private final Class<? extends Message> msgCls;

    /**
     * @param directType Direct type.
     */
    MessageType(int directType, Class<? extends Message> msgCls) {
        this.directType = (short)directType;
        this.msgCls = msgCls;
    }

    /** */
    static boolean isCalciteMessage(Message msg) {
        MessageType[] values = values();
        short msgType = msg.directType();

        return msgType >= values[0].directType() && msgType <= values[values.length - 1].directType();
    }

    /**
     * @return Message direct type.
     */
    public short directType() {
        return directType;
    }

    /**
     * @return Message direct type.
     */
    public Class<? extends Message> messageClass() {
        return msgCls;
    }
}
