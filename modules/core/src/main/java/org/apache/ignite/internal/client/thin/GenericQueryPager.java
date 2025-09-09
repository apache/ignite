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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientReconnectedException;
import org.apache.ignite.internal.client.thin.TcpClientTransactions.TcpClientTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Generic query pager. Override {@link this#readResult(PayloadInputChannel)} to make it specific.
 */
abstract class GenericQueryPager<T> implements QueryPager<T> {
    /** Query op. */
    private final ClientOperation qryOp;

    /** Query op. */
    private final ClientOperation pageQryOp;

    /** Query writer. */
    private final Consumer<PayloadOutputChannel> qryWriter;

    /** Channel. */
    private final ReliableChannel ch;

    /** Client Transaction. */
    private final @Nullable TcpClientTransaction tx;

    /** Has next. */
    private boolean hasNext = true;

    /** Indicates if initial query response was received. */
    private boolean hasFirstPage = false;

    /** Cursor id. */
    private Long cursorId = null;

    /** Client channel on first query page. */
    private ClientChannel clientCh;

    /** Cache ID, required only for affinity node calculation. */
    private final int cacheId;

    /** Partition filter (-1 for all partitions), required only for affinity node calculation. */
    private final int part;

    /** Constructor. */
    GenericQueryPager(
        ReliableChannel ch,
        @Nullable TcpClientTransaction tx,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter,
        int cacheId,
        int part
    ) {
        this.ch = ch;
        this.tx = tx;
        this.qryOp = qryOp;
        this.pageQryOp = pageQryOp;
        this.qryWriter = qryWriter;
        this.cacheId = cacheId;
        this.part = part;
    }

    /** Constructor. */
    GenericQueryPager(
        ReliableChannel ch,
        @Nullable TcpClientTransaction tx,
        ClientOperation qryOp,
        ClientOperation pageQryOp,
        Consumer<PayloadOutputChannel> qryWriter
    ) {
        this(ch, tx, qryOp, pageQryOp, qryWriter, 0, -1);
    }

    /** {@inheritDoc} */
    @Override public Collection<T> next() throws ClientException {
        if (!hasNext)
            throw new IllegalStateException("No more query results");

        if (hasFirstPage)
            return queryPage();

        if (tx != null && tx.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.TX_AWARE_QUERIES))
            return tx.clientChannel().service(qryOp, qryWriter, this::readResult);

        return part == -1
                ? ch.service(qryOp, qryWriter, this::readResult)
                : ch.affinityService(cacheId, part, qryOp, qryWriter, this::readResult);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // Close cursor only if the server has more pages: the server closes cursor automatically on last page
        if (cursorId != null && hasNext && !clientCh.closed()) {
            try {
                clientCh.service(ClientOperation.RESOURCE_CLOSE, req -> req.out().writeLong(cursorId), null);
            }
            catch (ClientConnectionException | ClientReconnectedException ignored) {
                // Original connection was lost and cursor was closed by the server.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public boolean hasFirstPage() {
        return hasFirstPage;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        hasFirstPage = false;

        hasNext = true;

        cursorId = null;

        clientCh = null;
    }

    /**
     * Override this method to read entries from the input stream. "Entries" means response data excluding heading
     * cursor ID and trailing "has next page" flag.
     * Use {@link this#hasFirstPage} flag to differentiate between the initial query and page query responses.
     */
    abstract Collection<T> readEntries(PayloadInputChannel in);

    /** */
    private Collection<T> readResult(PayloadInputChannel payloadCh) {
        if (!hasFirstPage) {
            long resCursorId = payloadCh.in().readLong();

            if (cursorId != null) {
                if (cursorId != resCursorId)
                    throw new ClientProtocolError(
                        String.format("Expected cursor [%s] but received cursor [%s]", cursorId, resCursorId)
                    );
            }
            else {
                cursorId = resCursorId;

                clientCh = payloadCh.clientChannel();
            }
        }

        Collection<T> res = readEntries(payloadCh);

        hasNext = payloadCh.in().readBoolean();

        hasFirstPage = true;

        return res;
    }

    /** Get page. */
    private Collection<T> queryPage() throws ClientException {
        return clientCh.service(pageQryOp, req -> req.out().writeLong(cursorId), this::readResult);
    }
}
