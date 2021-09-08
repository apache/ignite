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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.value.Value;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * Per node range stream.
 */
public class RangeStream {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Index. */
    private final H2TreeIndex idx;

    /** */
    private final DistributedJoinContext joinCtx;

    /** */
    private final ClusterNode node;

    /** */
    private GridH2IndexRangeRequest req;

    /** */
    private int remainingRanges;

    /** */
    private final BlockingQueue<GridH2IndexRangeResponse> respQueue = new LinkedBlockingQueue<>();

    /** */
    private Iterator<GridH2RowRange> ranges = emptyIterator();

    /** */
    private Cursor cursor = GridH2Cursor.EMPTY;

    /** */
    private int cursorRangeId = -1;

    /**
     * @param joinCtx Join context.
     * @param node Node.
     */
    public RangeStream(GridKernalContext ctx, H2TreeIndex idx, DistributedJoinContext joinCtx, ClusterNode node) {
        this.ctx = ctx;
        this.idx = idx;
        this.node = node;
        this.joinCtx = joinCtx;
    }

    /**
     * Start streaming.
     */
    public void start() {
        remainingRanges = req.bounds().size();

        assert remainingRanges > 0;

        idx.send(singletonList(node), req);
    }

    /**
     * @param msg Response.
     */
    public void onResponse(GridH2IndexRangeResponse msg) {
        respQueue.add(msg);
    }

    /**
     * @param req Current request.
     */
    public void request(GridH2IndexRangeRequest req) {
        this.req = req;
    }

    /**
     * @return Current request.
     */
    public GridH2IndexRangeRequest request() {
        return req;
    }

    /**
     * @return Response.
     */
    private GridH2IndexRangeResponse awaitForResponse() {
        assert remainingRanges > 0;

        final long start = U.currentTimeMillis();

        for (int attempt = 0;; attempt++) {
            if (joinCtx.isCancelled())
                throw H2Utils.retryException("Query is cancelled.");

            if (ctx.isStopping())
                throw H2Utils.retryException("Local node is stopping.");

            GridH2IndexRangeResponse res;

            try {
                res = respQueue.poll(500, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                throw H2Utils.retryException("Interrupted while waiting for reply.");
            }

            if (res != null) {
                switch (res.status()) {
                    case STATUS_OK:
                        List<GridH2RowRange> ranges0 = res.ranges();

                        remainingRanges -= ranges0.size();

                        if (ranges0.get(ranges0.size() - 1).isPartial())
                            remainingRanges++;

                        if (remainingRanges > 0) {
                            if (req.bounds() != null)
                                req = DistributedLookupBatch.createRequest(joinCtx, req.batchLookupId(), req.segment());

                            // Prefetch next page.
                            idx.send(singletonList(node), req);
                        }
                        else
                            req = null;

                        return res;

                    case STATUS_NOT_FOUND:
                        if (req == null || req.bounds() == null) // We have already received the first response.
                            throw H2Utils.retryException("Failure on remote node.");

                        if (U.currentTimeMillis() - start > 30_000)
                            throw H2Utils.retryException("Timeout reached.");

                        try {
                            U.sleep(20 * attempt);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteInterruptedException(e.getMessage());
                        }

                        // Retry to send the request once more after some time.
                        idx.send(singletonList(node), req);

                        break;

                    case STATUS_ERROR:
                        throw new CacheException(res.error());

                    default:
                        throw new IllegalStateException();
                }
            }

            if (!ctx.discovery().alive(node))
                throw H2Utils.retryException("Node has left topology: " + node.id());
        }
    }

    /**
     * @param rangeId Requested range ID.
     * @return {@code true} If next row for the requested range was found.
     */
    public boolean next(final int rangeId) {
        for (;;) {
            if (rangeId == cursorRangeId) {
                if (cursor.next())
                    return true;
            }
            else if (rangeId < cursorRangeId)
                return false;

            cursor = GridH2Cursor.EMPTY;

            while (!ranges.hasNext()) {
                if (remainingRanges == 0) {
                    ranges = emptyIterator();

                    return false;
                }

                ranges = awaitForResponse().ranges().iterator();
            }

            GridH2RowRange range = ranges.next();

            cursorRangeId = range.rangeId();

            if (!F.isEmpty(range.rows())) {
                final Iterator<GridH2RowMessage> it = range.rows().iterator();

                if (it.hasNext()) {
                    cursor = new GridH2Cursor(new Iterator<Row>() {
                        @Override public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override public Row next() {
                            // Lazily convert messages into real rows.
                            return toRow(it.next());
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    });
                }
            }
        }
    }

    /**
     * @param msg Message.
     * @return Row.
     */
    private Row toRow(GridH2RowMessage msg) {
        if (msg == null)
            return null;

        List<GridH2ValueMessage> vals = msg.values();

        assert !F.isEmpty(vals) : vals;

        Value[] vals0 = new Value[vals.size()];

        for (int i = 0; i < vals0.length; i++) {
            try {
                vals0[i] = vals.get(i).value(ctx);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        return idx.getDatabase().createRow(vals0, MEMORY_CALCULATE);
    }

    /**
     * @param rangeId Requested range ID.
     * @return Current row.
     */
    public Row get(int rangeId) {
        assert rangeId == cursorRangeId;

        return cursor.get();
    }
}
