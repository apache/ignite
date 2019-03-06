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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientConnectionException;

/**
 * Generic query pager. Override {@link this#readResult(BinaryInputStream)} to make it specific.
 */
abstract class GenericQueryPager<T> implements QueryPager<T> {
    /** Query op. */
    private final ClientOperation qryOp;

    /** Query op. */
    private final ClientOperation pageQryOp;

    /** Query writer. */
    private final Consumer<BinaryOutputStream> qryWriter;

    /** Channel. */
    private final ReliableChannel ch;

    /** Has next. */
    private boolean hasNext = true;

    /** Indicates if initial query response was received. */
    private boolean hasFirstPage = false;

    /** Cursor id. */
    private Long cursorId = null;

    /** Constructor. */
    GenericQueryPager(
        ReliableChannel ch,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<BinaryOutputStream> qryWriter
    ) {
        this.ch = ch;
        this.qryOp = qryOp;
        this.pageQryOp = pageQryOp;
        this.qryWriter = qryWriter;
    }

    /** {@inheritDoc} */
    @Override public Collection<T> next() throws ClientException {
        if (!hasNext)
            throw new IllegalStateException("No more query results");

        return hasFirstPage ? queryPage() : ch.service(qryOp, qryWriter, this::readResult);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // Close cursor only if the server has more pages: the server closes cursor automatically on last page
        if (cursorId != null && hasNext)
            ch.request(ClientOperation.RESOURCE_CLOSE, req -> req.writeLong(cursorId));
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public boolean hasFirstPage() {
        return hasFirstPage;
    }

    /**
     * Override this method to read entries from the input stream. "Entries" means response data excluding heading
     * cursor ID and trailing "has next page" flag.
     * Use {@link this#hasFirstPage} flag to differentiate between the initial query and page query responses.
     */
    abstract Collection<T> readEntries(BinaryInputStream in);

    /** */
    private Collection<T> readResult(BinaryInputStream in) {
        if (!hasFirstPage) {
            long resCursorId = in.readLong();

            if (cursorId != null) {
                if (cursorId != resCursorId)
                    throw new ClientProtocolError(
                        String.format("Expected cursor [%s] but received cursor [%s]", cursorId, resCursorId)
                    );
            }
            else
                cursorId = resCursorId;
        }

        Collection<T> res = readEntries(in);

        hasNext = in.readBoolean();

        hasFirstPage = true;

        return res;
    }

    /** Get page with failover. */
    private Collection<T> queryPage() throws ClientException {
        try {
            return ch.service(pageQryOp, req -> req.writeLong(cursorId), this::readResult);
        }
        catch (ClientServerError ex) {
            if (ex.getCode() != ClientStatus.RESOURCE_DOES_NOT_EXIST)
                throw ex;
        }
        catch (ClientConnectionException ignored) {
        }

        // Retry entire query to failover
        hasFirstPage = false;

        return ch.service(qryOp, qryWriter, this::readResult);
    }
}
