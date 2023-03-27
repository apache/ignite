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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class DataPagePayload {
    /** */
    private final int off;

    /** */
    private final int payloadSize;

    /** */
    private final long nextLink;

    /**
     * @param off Offset.
     * @param payloadSize Payload size.
     * @param nextLink Next link.
     */
    DataPagePayload(int off, int payloadSize, long nextLink) {
        this.off = off;
        this.payloadSize = payloadSize;
        this.nextLink = nextLink;
    }

    /**
     * @return Offset.
     */
    public int offset() {
        return off;
    }

    /**
     * @return Payload size.
     */
    public int payloadSize() {
        return payloadSize;
    }

    /**
     * @return Link to the next fragment or {@code 0} if it is the last fragment or the data row is not fragmented.
     */
    public long nextLink() {
        return nextLink;
    }

    /**
     * @param pageAddr Page address.
     * @return Payload bytes.
     */
    public byte[] getBytes(long pageAddr) {
        return PageUtils.getBytes(pageAddr, off, payloadSize);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPagePayload.class, this);
    }
}
