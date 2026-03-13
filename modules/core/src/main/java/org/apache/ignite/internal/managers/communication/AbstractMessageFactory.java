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

package org.apache.ignite.internal.managers.communication;

import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.AbstractMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;

/** Abstract message factory. */
public abstract class AbstractMessageFactory implements MessageFactoryProvider {
    /** Temporary Message factory holder to do some work with. */
    private MessageFactory factory;

    /** {@inheritDoc} */
    @Override public final void registerAll(MessageFactory factory) {
        this.factory = factory;

        doRegisterAllMessages(factory);

        this.factory = null;
    }

    /** */
    protected abstract void doRegisterAllMessages(MessageFactory factory);

    /** */
    protected void register(Class<? extends AbstractMessage> msgCls) {
        assert factory != null;

        Supplier<Message> msgSupp = () -> {
            try {
                AbstractMessage msg = msgCls.getConstructor().newInstance();

                return msg;
            }
            catch (Exception e) {
                throw new IgniteException("Unable to create message of type " + msgCls.getSimpleName(), e);
            }
        };

        MessageSerializer msgSerr;

        try {
            msgSerr = (MessageSerializer)Class.forName(msgCls.getName() + "Serializer").getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new IgniteException("Unable to create message serializer for message of type " + msgCls.getSimpleName(), e);
        }

        factory.register(msgSupp.get().directType(), msgSupp, msgSerr);
    }
}
