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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data page update result.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class DataPageUpdateResult {
    /** */
    private final int payloadSize;

    /** */
    private final byte[] payload;

    /** */
    private final long nextLink;

    /**
     * @param payloadSize Payload size.
     * @param payload Payload, if it was modified or {@code null}.
     * @param nextLink Next link.
     */
    DataPageUpdateResult(int payloadSize, byte[] payload, long nextLink) {
        this.payloadSize = payloadSize;
        this.payload = payload;
        this.nextLink = nextLink;
    }

    /**
     * @return Modified payload.
     */
    public byte[] modifiedPayload() {
        return payload;
    }

    /**
     * @return Link to the next fragment or {@code 0} if it is the last fragment or the data row is not fragmented.
     */
    public long nextLink() {
        return nextLink;
    }

    /**
     * @return Payload size.
     */
    public int payloadSize() {
        return payloadSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageUpdateResult.class, this);
    }
}
