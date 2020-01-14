/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.message;


import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.GENERIC_ROW_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_ACKNOWLEDGE_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_BATCH_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_CANCEL_REQUEST;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_INBOX_CANCEL_MESSAGE;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_START_REQUEST;
import static org.apache.ignite.internal.processors.query.calcite.message.MessageType.QUERY_START_RESPONSE;

/**
 * Message factory.
 */
public class CalciteMessageFactory implements MessageFactory {
    /** {@inheritDoc} */
    @Override public @Nullable Message create(short type) {
        switch (type) {
            case QUERY_START_REQUEST:
                return new QueryStartRequest();
            case QUERY_START_RESPONSE:
                return new QueryStartResponse();
            case QUERY_CANCEL_REQUEST:
                return new QueryCancelRequest();
            case QUERY_BATCH_MESSAGE:
                return new QueryBatchMessage();
            case QUERY_ACKNOWLEDGE_MESSAGE:
                return new QueryBatchAcknowledgeMessage();
            case QUERY_INBOX_CANCEL_MESSAGE:
                return new InboxCancelMessage();
            case GENERIC_ROW_MESSAGE:
                return new GenericRowMessage();
            default:
                return null;
        }
    }

    /** */
    public static Message asMessage(Object row) {
        return new GenericRowMessage(row);
    }

    /** */
    public static Object asRow(Message mRow) {
        if (mRow instanceof GenericRowMessage)
            return ((GenericRowMessage) mRow).row();

        throw new AssertionError("Unexpected message type. [message=" + mRow + "]");
    }
}
