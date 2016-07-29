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

import java.nio.ByteBuffer;

/**
 * Cache object container that accumulates partial binary data
 * unless all of them ready.
 *
 * @param <T> Cache object type.
 */
public class IncompleteCacheObject<T extends CacheObject> {
    /** 4 bytes - cache object length, 1 byte - type. */
    public static final int HEAD_LEN = 5;

    /** */
    private byte[] data;

    /** */
    private byte type;

    /** */
    private int off;

    /** */
    private T obj;

    /** */
    private int headOff;

    /** */
    private byte[] head;

    /**
     * @param data Data array.
     * @param type Data type.
     */
    public IncompleteCacheObject(final byte[] data, final byte type) {
        this.data = data;
        this.type = type;
    }

    /**
     * @param buf Byte buffer.
     */
    public IncompleteCacheObject(final ByteBuffer buf) {
        if (buf.remaining() >= HEAD_LEN) {
            data = new byte[buf.getInt()];
            type = buf.get();
        }
        // We cannot fully read head to initialize data buffer.
        // Start partial read of header.
        else
            head = new byte[HEAD_LEN];
    }

    /**
     * @return {@code True} if cache object is fully assembled.
     */
    public boolean isReady() {
        return data != null && off == data.length;
    }

    /**
     * @return Cache object if ready.
     */
    public T cacheObject() {
        return obj;
    }

    /**
     * @param cacheObj Cache object.
     */
    public void cacheObject(T cacheObj) {
        obj = cacheObj;
    }

    /**
     * @param buf Read remaining data.
     */
    public void readData(ByteBuffer buf) {
        if (data == null) {
            assert head != null;

            final int len = Math.min(HEAD_LEN - headOff, buf.remaining());

            buf.get(head, headOff, len);

            headOff += len;

            if (headOff == HEAD_LEN) {
                final ByteBuffer headBuf = ByteBuffer.wrap(head);

                headBuf.order(buf.order());

                data = new byte[headBuf.getInt()];
                type = headBuf.get();
            }
        }

        if (data != null) {
            final int len = Math.min(data.length - off, buf.remaining());

            buf.get(data, off, len);

            off += len;
        }
    }

    /**
     * @return Data type.
     */
    public byte type() {
        return type;
    }

    /**
     * @return Data array.
     */
    public byte[] data() {
        return data;
    }
}
