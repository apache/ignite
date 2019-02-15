/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

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
