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

import javax.cache.Cache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;

/**
 * Query cursor holder.
  */
class ClientCacheEntryQueryCursor extends ClientCacheQueryCursor<Cache.Entry> {
    /**
     * Ctor.
     *
     * @param cursor   Cursor.
     * @param pageSize Page size.
     * @param ctx      Context.
     */
    ClientCacheEntryQueryCursor(QueryCursor<Cache.Entry> cursor, int pageSize, ClientConnectionContext ctx) {
        super(cursor, pageSize, ctx);
    }

    /** {@inheritDoc} */
    @Override void writeEntry(BinaryRawWriterEx writer, Cache.Entry e) {
        writer.writeObjectDetached(e.getKey());
        writer.writeObjectDetached(e.getValue());
    }
}
