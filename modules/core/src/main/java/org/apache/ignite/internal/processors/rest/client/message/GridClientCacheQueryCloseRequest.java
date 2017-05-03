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
 * Close cursor cache query request.
 */
public class GridClientCacheQueryCloseRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cursor ID for not empty. */
    private long curId;

    /**
     * Default constructor needs for Externalizable.
     */
    public GridClientCacheQueryCloseRequest() {
        // No-op.
    }

    /**
     * The request is used for paging with the opened cursor.
     *
     * @param curId Cursor ID.
     */
    public GridClientCacheQueryCloseRequest(Long curId) {
        this.curId = curId;
    }

    /**
     * @return Cursor ID for not empty.
     */
    public long cursorId() {
        return curId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(curId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        curId = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientCacheQueryCloseRequest.class, this);
    }
}