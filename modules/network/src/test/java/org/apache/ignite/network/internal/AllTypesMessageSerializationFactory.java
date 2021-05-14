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

package org.apache.ignite.network.internal;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializationFactory;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * {@link MessageSerializationFactory} for an {@link AllTypesMessage}.
 */
public class AllTypesMessageSerializationFactory implements MessageSerializationFactory<AllTypesMessage> {
    /** {@inheritDoc} */
    @Override public MessageDeserializer<AllTypesMessage> createDeserializer() {
        return new MessageDeserializer<AllTypesMessage>() {
            /** */
            AllTypesMessage msg = new AllTypesMessage();

            /** */
            byte a;

            /** */
            short b;

            /** */
            int c;

            /** */
            long d;

            /** */
            float e;

            /** */
            double f;

            /** */
            char g;

            /** */
            boolean h;

            /** */
            byte[] i;

            /** */
            short[] j;

            /** */
            int[] k;

            /** */
            long[] l;

            /** */
            float[] m;

            /** */
            double[] n;

            /** */
            char[] o;

            /** */
            boolean[] p;

            /** */
            String q;

            /** */
            BitSet r;

            /** */
            UUID s;

            /** */
            IgniteUuid t;

            /** */
            NetworkMessage u;

            /** */
            Object[] v;

            /** */
            Collection<?> w;

            /** */
            Map<?, ?> x;

            /** {@inheritDoc} */
            @Override public boolean readMessage(MessageReader reader) throws MessageMappingException {
                if (!reader.beforeMessageRead())
                    return false;

                switch (reader.state()) {
                    case 0:
                        a = reader.readByte("a");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 1:
                        b = reader.readShort("b");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 2:
                        c = reader.readInt("c");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 3:
                        d = reader.readLong("d");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 4:
                        e = reader.readFloat("e");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 5:
                        f = reader.readDouble("f");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 6:
                        g = reader.readChar("g");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 7:
                        h = reader.readBoolean("h");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 8:
                        i = reader.readByteArray("i");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 9:
                        j = reader.readShortArray("j");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 10:
                        k = reader.readIntArray("k");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 11:
                        l = reader.readLongArray("l");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 12:
                        m = reader.readFloatArray("m");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 13:
                        n = reader.readDoubleArray("n");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 14:
                        o = reader.readCharArray("o");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 15:
                        p = reader.readBooleanArray("p");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 16:
                        q = reader.readString("q");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 17:
                        r = reader.readBitSet("r");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 18:
                        s = reader.readUuid("s");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 19:
                        t = reader.readIgniteUuid("t");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 20:
                        u = reader.readMessage("u");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 21:
                        v = reader.readObjectArray("v", MessageCollectionItemType.MSG, AllTypesMessage.class);

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 22:
                        w = reader.readCollection("w", MessageCollectionItemType.MSG);

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 23:
                        x = reader.readMap("x", MessageCollectionItemType.STRING, MessageCollectionItemType.MSG, false);

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                }

                msg.a = a;
                msg.b = b;
                msg.c = c;
                msg.d = d;
                msg.e = e;
                msg.f = f;
                msg.g = g;
                msg.h = h;
                msg.i = i;
                msg.j = j;
                msg.k = k;
                msg.l = l;
                msg.m = m;
                msg.n = n;
                msg.o = o;
                msg.p = p;
                msg.q = q;
                msg.r = r;
                msg.s = s;
                msg.t = t;
                msg.u = u;
                msg.v = v;
                msg.w = w;
                msg.x = x;

                this.msg = msg;

                return reader.afterMessageRead(TestMessage.class);
            }

            /** {@inheritDoc} */
            @Override public Class<AllTypesMessage> klass() {
                return AllTypesMessage.class;
            }

            /** {@inheritDoc} */
            @Override public AllTypesMessage getMessage() {
                return msg;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer<AllTypesMessage> createSerializer() {
        return (message, writer) -> {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(message.directType(), (byte) 24))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeByte("a", message.a))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 1:
                    if (!writer.writeShort("b", message.b))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 2:
                    if (!writer.writeInt("c", message.c))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 3:
                    if (!writer.writeLong("d", message.d))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 4:
                    if (!writer.writeFloat("e", message.e))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 5:
                    if (!writer.writeDouble("f", message.f))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 6:
                    if (!writer.writeChar("g", message.g))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 7:
                    if (!writer.writeBoolean("h", message.h))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 8:
                    if (!writer.writeByteArray("i", message.i))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 9:
                    if (!writer.writeShortArray("j", message.j))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 10:
                    if (!writer.writeIntArray("k", message.k))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 11:
                    if (!writer.writeLongArray("l", message.l))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 12:
                    if (!writer.writeFloatArray("m", message.m))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 13:
                    if (!writer.writeDoubleArray("n", message.n))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 14:
                    if (!writer.writeCharArray("o", message.o))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 15:
                    if (!writer.writeBooleanArray("p", message.p))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 16:
                    if (!writer.writeString("q", message.q))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 17:
                    if (!writer.writeBitSet("r", message.r))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 18:
                    if (!writer.writeUuid("s", message.s))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 19:
                    if (!writer.writeIgniteUuid("t", message.t))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 20:
                    if (!writer.writeMessage("u", message.u))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 21:
                    if (!writer.writeObjectArray("v", message.v, MessageCollectionItemType.MSG))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 22:
                    if (!writer.writeCollection("w", message.w, MessageCollectionItemType.MSG))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 23:
                    if (!writer.writeMap("x", message.x, MessageCollectionItemType.STRING, MessageCollectionItemType.MSG))
                        return false;

                    writer.incrementState();
            }

            return true;
        };
    }
}
