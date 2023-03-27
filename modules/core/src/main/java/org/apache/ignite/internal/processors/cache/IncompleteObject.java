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
 * Incomplete object.
 */
public class IncompleteObject<T> {
    /** */
    private long nextLink;

    /** */
    protected byte[] data;

    /** */
    private T obj;

    /** */
    private int off;

    /**
     * @param data Data bytes.
     */
    public IncompleteObject(byte[] data) {
        this.data = data;
    }

    /**
     * Constructor.
     */
    public IncompleteObject() {
        // No-op.
    }

    /**
     * @return Cache object if ready.
     */
    public T object() {
        return obj;
    }

    /**
     * @param obj Cache object.
     */
    public void object(T obj) {
        this.obj = obj;
    }

    /**
     * @return {@code True} if cache object is fully assembled.
     */
    public boolean isReady() {
        return data != null && off == data.length;
    }

    /**
     * @return Data array.
     */
    public byte[] data() {
        return data;
    }

    /**
     * @param buf Read remaining data.
     */
    public void readData(ByteBuffer buf) {
        assert data != null;

        final int len = Math.min(data.length - off, buf.remaining());

        buf.get(data, off, len);

        off += len;
    }

    /**
     * @return Next data page link for fragmented rows.
     */
    public long getNextLink() {
        return nextLink;
    }

    /**
     * @param nextLink Next data page link for fragmented rows.
     */
    public void setNextLink(long nextLink) {
        this.nextLink = nextLink;
    }
}
