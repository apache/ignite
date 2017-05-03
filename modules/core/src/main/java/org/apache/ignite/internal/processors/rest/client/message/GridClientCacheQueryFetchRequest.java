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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Fetch result cache query request.
 */
public class GridClientCacheQueryFetchRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cursor ID for not empty. */
    private long curId;

    /** Page size. */
    private int pageSize;

    /**
     * Default constructor needs for Externalizable.
     */
    public GridClientCacheQueryFetchRequest() {
        // No-op.
    }

    /**
     * The request is used for paging with the opened cursor.
     *
     * @param curId Cursor ID.
     * @param pageSize Page size.
     */
    public GridClientCacheQueryFetchRequest(Long curId, int pageSize) {
        this.curId = curId;
        this.pageSize = pageSize;
    }

    /**
     * @return Cursor ID for not empty.
     */
    public long cursorId() {
        return curId;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }


    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(curId);
        out.writeInt(pageSize);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        curId = in.readLong();
        pageSize = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientCacheQueryFetchRequest.class, this);
    }
}