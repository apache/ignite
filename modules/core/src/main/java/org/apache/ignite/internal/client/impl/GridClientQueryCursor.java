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

package org.apache.ignite.internal.client.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryResult;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.jetbrains.annotations.NotNull;

/**
 * Class contains common connection-error handling logic.
 */
public class GridClientQueryCursor extends GridClientAbstractProjection<GridClientDataImpl>
    implements QueryCursor<List<Object>> {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientQueryCursor.class.getName());

    /** Query result. */
    private CacheQueryResult queryRes;

    /** Page size. */
    private int pageSize;

    /**
     * Creates projection with specified client.
     * @param client Client instance to use.
     * @param nodes Collections of nodes included in this projection.
     * @param filter Node filter to be applied.
     * @param balancer Balancer to use.
     * @param queryRes Query results.
     * @param pageSize Page size
     */
    GridClientQueryCursor(GridClientImpl client,
        Collection<GridClientNode> nodes,
        GridClientPredicate<? super GridClientNode> filter,
        GridClientLoadBalancer balancer,
        CacheQueryResult queryRes, int pageSize) {
        super(client, nodes, filter, balancer);

        this.queryRes = queryRes;
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public List<List<Object>> getAll() {
        List<List<Object>> res = new ArrayList<>();

        for (List<Object> objects : this)
            res.add(objects);

        close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            if (queryRes == null)
                return;

            Boolean res = withReconnectHandling(
                new ClientProjectionClosure<Boolean>() {
                    @Override public GridClientFuture<Boolean> apply(GridClientConnection conn,
                        UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
                        return conn.queryClose(queryRes.getQueryId());
                    }
                }).get();

            if (!res)
                throw new IgniteException("Error on query close: " + queryRes);
        }
        catch (GridClientException e) {
            throw new IgniteException("Error on query close: " + queryRes, e);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<List<Object>> iterator() {
        if (queryRes == null)
            return new GridEmptyIterator<>();

        return new Iterator<List<Object>>() {
            private Iterator it = queryRes.getItems().iterator();

            @Override public boolean hasNext() {
                if (!it.hasNext() && queryRes != null && !queryRes.getLast()) {
                    try {
                        CacheQueryResult res = withReconnectHandling(
                            new ClientProjectionClosure<CacheQueryResult>() {
                                @Override public GridClientFuture<CacheQueryResult> apply(GridClientConnection conn,
                                    UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
                                    return conn.queryFetch(queryRes.getQueryId(), pageSize);
                                }
                            }).get();

                        queryRes = res;

                        if (queryRes != null)
                            it = queryRes.getItems().iterator();
                    }
                    catch (GridClientException e) {
                        throw new IgniteException("Error on query fetch: " + queryRes, e);
                    }
                }

                return queryRes != null && it.hasNext();
            }

            @Override public List<Object> next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return (List<Object>)it.next();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException("Remove is unsupported for this iterator");
            }
        };
    }
}