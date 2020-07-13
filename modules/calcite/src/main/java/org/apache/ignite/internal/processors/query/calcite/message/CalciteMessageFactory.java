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
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;

/**
 * Message factory.
 */
public class CalciteMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override public void registerAll(IgniteMessageFactory factory) {
        for (MessageType type : MessageType.values())
            factory.register(type.directType(), (Supplier)type.factory());
    }

    /**
     * Produces a row message.
     *
     * TODO In future a row is expected to implement Message interface.
     */
    public static Message asMessage(Object row) {
        return new GenericRowMessage(row);
    }

    /**
     * Produces a row from a message.
     *
     * TODO In future a row is expected to implement Message interface.
     */
    public static Object asRow(Message mRow) {
        if (mRow instanceof GenericRowMessage)
            return ((GenericRowMessage) mRow).row();

        throw new AssertionError("Unexpected message type. [message=" + mRow + "]");
    }
}
