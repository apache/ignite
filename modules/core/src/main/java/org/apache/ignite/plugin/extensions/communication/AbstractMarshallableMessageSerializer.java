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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.marshaller.Marshaller;

/** {@link MarshallableMessage} aware message marshaller. */
abstract class AbstractMarshallableMessageSerializer<AM extends MarshallableMessage> implements MessageSerializer<AM> {
    /** */
    private final MessageSerializer<AM> delegate;

    /** */
    private final Marshaller marshaller;

    /** */
    private final ClassLoader clsLdr;

    /** */
    private boolean writeBegun;

    /** */
    protected AbstractMarshallableMessageSerializer(MessageSerializer<AM> delegate, Marshaller marshaller, ClassLoader clsLdr) {
        this.delegate = delegate;
        this.marshaller = marshaller;
        this.clsLdr = clsLdr;
    }

    /** */
    @Override public boolean writeTo(AM msg, MessageWriter writer) {
        if (!writeBegun) {
            writeBegun = true;

            msg.prepareMarshal(marshaller);
        }

        boolean res = delegate.writeTo(msg, writer);

        if (res)
            writeBegun = false;

        return res;
    }

    /** */
    @Override public boolean readFrom(AM msg, MessageReader reader) {
        boolean res = delegate.readFrom(msg, reader);

        if (res)
            msg.finishUnmarshal(marshaller, clsLdr);

        return res;
    }
}
