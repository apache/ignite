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

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Scan query response.
 */
class ClientCacheSqlFieldsQueryResponse extends ClientResponse {
    /** Cursor. */
    private final ClientCacheQueryCursor cursor;

    /** Fields cursor. */
    private final FieldsQueryCursor<List> fieldsCursor;

    /** Include field names flag. */
    private final boolean includeFieldNames;

    /**
     * Ctor.
     * @param requestId Request id.
     * @param cursor Client cursor.
     * @param fieldsCursor Fields cursor.
     * @param includeFieldNames Whether to include field names.
     */
    ClientCacheSqlFieldsQueryResponse(long requestId, ClientCacheQueryCursor cursor,
                                      FieldsQueryCursor<List> fieldsCursor, boolean includeFieldNames) {
        super(requestId);

        assert cursor != null;
        assert fieldsCursor != null;

        this.cursor = cursor;
        this.fieldsCursor = fieldsCursor;
        this.includeFieldNames = includeFieldNames;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeLong(cursor.id());

        int cnt = fieldsCursor.getColumnsCount();
        writer.writeInt(cnt);

        if (includeFieldNames) {
            for (int i = 0; i < cnt; i++) {
                writer.writeString(fieldsCursor.getFieldName(i));
            }
        }

        cursor.writePage(writer);
    }
}
