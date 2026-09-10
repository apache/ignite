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

import java.util.EnumSet;
import java.util.function.Function;

/** */
public class MessageEnumType<T extends Enum<T>> implements MessageType {
    /** */
    private final Class<T> cls;

    /** */
    private final Function<T, Byte> encoder;

    /** */
    private final Function<Byte, T> decoder;

    /**
     * @param cls Enum class.
     * @param encoder Encoder.
     * @param decoder Decoder.
     */
    public MessageEnumType(Class<T> cls, Function<T, Byte> encoder, Function<Byte, T> decoder) {
        this.cls = cls;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    /** @return Empty {@link EnumSet} of the enum type. */
    public EnumSet<T> newEnumSet() {
        return EnumSet.noneOf(cls);
    }

    /**
     * @param val Value.
     * @return Encoded value.
     */
    public byte encode(T val) {
        return encoder.apply(val);
    }

    /**
     * @param b Byte representation of enum value.
     * @return Corresponding enum value.
     */
    public T decode(byte b) {
        return decoder.apply(b);
    }

    /** {@inheritDoc} */
    @Override public MessageCollectionItemType type() {
        return MessageCollectionItemType.ENUM;
    }
}
