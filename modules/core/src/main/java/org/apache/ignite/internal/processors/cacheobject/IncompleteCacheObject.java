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

package org.apache.ignite.internal.processors.cacheobject;

import org.apache.ignite.internal.processors.cache.CacheObject;

import java.nio.ByteBuffer;

/**
 * Cache object container that accumulates partial binary data
 * unless all of them ready.
 *
 * @param <T> Cache object type.
 */
public class IncompleteCacheObject<T extends CacheObject> {
    /** */
    private final byte[] data;

    /** */
    private final byte type;

    /** */
    private int offset;

    /** */
    private T obj;

    /**
     * @param data Data array.
     * @param type Data type.
     */
    public IncompleteCacheObject(final byte[] data, final byte type) {
        this.data = data;
        this.type = type;
    }

    /**
     * @return {@code True} if cache object is fully assembled.
     */
    public boolean isReady() {
        return offset == data.length;
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
    void cacheObject(T cacheObj) {
        obj = cacheObj;
    }

    /**
     * @param buf Read remaining data.
     */
    public void readData(ByteBuffer buf) {
        final int len = Math.min(data.length - offset, buf.remaining());

        buf.get(data, offset, len);

        offset += len;
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
