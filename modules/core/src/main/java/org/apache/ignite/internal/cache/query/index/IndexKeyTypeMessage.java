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

package org.apache.ignite.internal.cache.query.index;

import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message wrapper for {@link IndexKeyType}. See {@link MessageProcessor} for details.
 */
public class IndexKeyTypeMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 516;

    /** */
    public static final byte NULL_VALUE_CODE = Byte.MIN_VALUE;

    /** Index key type. */
    private @Nullable IndexKeyType val;

    /** Code. */
    @Order(0)
    private byte code = NULL_VALUE_CODE;

    /**
     * Constructor.
     */
    public IndexKeyTypeMessage() {
        // No-op.
    }

    /**
     * Constructor.
     */
    public IndexKeyTypeMessage(@Nullable IndexKeyType keyType) {
        val = keyType;
        code = encode(keyType);
    }

    /**
     * Constructor.
     */
    public IndexKeyTypeMessage(int keyTypeCode) {
        code((byte)keyTypeCode);
    }

    /**
     * @return Code.
     */
    public byte code() {
        return code;
    }

    /**
     * @param code New code.
     */
    public void code(byte code) {
        this.code = code;
        val = decode(code);
    }

    /**
     * @return Index key type.
     */
    public @Nullable IndexKeyType value() {
        return val;
    }

    /** @param keyType Index key type. */
    private static byte encode(@Nullable IndexKeyType keyType) {
        if (keyType == null)
            return NULL_VALUE_CODE;

        return (byte)keyType.code();
    }

    /** @param code Code to decode an inde key type. */
    private static @Nullable IndexKeyType decode(byte code) {
        if (code == NULL_VALUE_CODE)
            return null;

        return IndexKeyType.forCode(code);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
