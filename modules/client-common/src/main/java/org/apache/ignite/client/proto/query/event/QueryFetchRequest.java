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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query fetch request.
 */
public class QueryFetchRequest implements ClientMessage {
    /** Cursor ID. */
    private long cursorId;

    /** Fetch size. */
    private int pageSize;

    /**
     * Constructor.
     */
    public QueryFetchRequest() {
    }

    /**
     * Constructor.
     *
     * @param cursorId Cursor ID.
     * @param pageSize Fetch size.
     */
    public QueryFetchRequest(long cursorId, int pageSize) {
        this.cursorId = cursorId;
        this.pageSize = pageSize;
    }

    /**
     * Get the cursor id.
     *
     * @return Cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * Get the fetch page size.
     *
     * @return Fetch page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packLong(cursorId);
        packer.packInt(pageSize);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        cursorId = unpacker.unpackLong();
        pageSize = unpacker.unpackInt();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(QueryFetchRequest.class, this);
    }
}
