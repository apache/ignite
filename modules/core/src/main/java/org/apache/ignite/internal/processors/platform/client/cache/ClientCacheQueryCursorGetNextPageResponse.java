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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import javax.cache.Cache;
import java.util.Iterator;

/**
 * Query cursor next page response.
 */
class ClientCacheQueryCursorGetNextPageResponse extends ClientResponse {
    /** Cursor. */
    private final ClientCacheQueryCursor<Cache.Entry> cursor;

    /**
     * Ctor.
     *
     * @param requestId Request id.
     * @param cursor Cursor.
     */
    ClientCacheQueryCursorGetNextPageResponse(int requestId, ClientCacheQueryCursor<Cache.Entry> cursor) {
        super(requestId);

        assert cursor != null;

        this.cursor = cursor;
    }

    /** {@inheritDoc} */
    @Override public void encode(BinaryRawWriterEx writer) {
        super.encode(writer);

        Iterator<Cache.Entry> iter = cursor.iterator();

        int pageSize = cursor.pageSize();
        int cntPos = writer.reserveInt();
        int cnt = 0;

        while (cnt < pageSize && iter.hasNext()) {
            Cache.Entry e = iter.next();

            writer.writeObjectDetached(e.getKey());
            writer.writeObjectDetached(e.getValue());

            cnt++;
        }

        writer.writeInt(cntPos, cnt);
    }
}
