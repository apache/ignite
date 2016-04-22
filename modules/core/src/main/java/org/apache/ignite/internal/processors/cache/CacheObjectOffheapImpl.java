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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 *
 */
public class CacheObjectOffheapImpl implements CacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long ptr;

    /** */
    private int size;

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
        byte[] res = new byte[size];

        return new byte[0];
    }

    /** {@inheritDoc} */
    @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
        return size + 5;
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(ByteBuffer buf, CacheObjectContext ctx) throws IgniteCheckedException {
        int pos = buf.position();
        int rem = buf.remaining();

        if (size > rem)
            return false;

        if (buf.isDirect()) {
            long ptr = ((DirectBuffer)buf).address();


        }
        else if (buf.hasArray()) {
            byte[] backingArr = buf.array();
            int off = buf.arrayOffset();


        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        return TYPE_REGULAR;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
