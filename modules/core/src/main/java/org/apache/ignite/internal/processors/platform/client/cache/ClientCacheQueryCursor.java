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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;

/**
 * Base query cursor holder.
  */
abstract class ClientCacheQueryCursor<T> implements ClientCloseableResource {
    /** Cursor. */
    private final QueryCursor<T> cursor;

    /** Page size. */
    private final int pageSize;

    /** Context. */
    private final ClientConnectionContext ctx;

    /** Id. */
    private long id;

    /** Iterator. */
    private Iterator<T> iterator;

    /** Close guard. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Ctor.
     *  @param cursor Cursor.
     * @param pageSize Page size.
     * @param ctx Context.
     */
    ClientCacheQueryCursor(QueryCursor<T> cursor, int pageSize, ClientConnectionContext ctx) {
        assert cursor != null;
        assert pageSize > 0;
        assert ctx != null;

        this.cursor = cursor;
        this.pageSize = pageSize;
        this.ctx = ctx;
    }

    /**
     * Writes next page to the writer.
     *
     * @param writer Writer.
     */
    void writePage(BinaryRawWriterEx writer) {
        Iterator<T> iter = iterator();

        int cntPos = writer.reserveInt();
        int cnt = 0;

        while (cnt < pageSize && iter.hasNext()) {
            T e = iter.next();

            writeEntry(writer, e);

            cnt++;
        }

        writer.writeInt(cntPos, cnt);

        writer.writeBoolean(iter.hasNext());

        if (!iter.hasNext())
            ctx.resources().release(id);
    }

    /**
     * Writes cursor entry.
     *
     * @param writer Writer.
     * @param e Entry.
     */
    abstract void writeEntry(BinaryRawWriterEx writer, T e);

    /**
     * Closes the cursor.
     */
    @Override public void close() {
        if (closeGuard.compareAndSet(false, true)) {
            cursor.close();

            ctx.decrementCursors();
        }
    }

    /**
     * Sets the cursor id.
     *
     * @param id Id.
     */
    public void id(long id) {
        this.id = id;
    }

    /**
     * Gets the cursor id.
     *
     * @return Id.
     */
    public long id() {
        return id;
    }

    /**
     * Gets the iterator.
     *
     * @return Iterator.
     */
    private Iterator<T> iterator() {
        if (iterator == null)
            iterator = cursor.iterator();

        return iterator;
    }
}
