/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.List;

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
