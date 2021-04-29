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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Page of result of cache query.
 */
public class CacheQueryResultPage<T> {
    /** */
    public final UUID nodeId;

    /** */
    private final GridCacheQueryResponse res;

    /** */
    private final int rowsInPage;

    /** */
    private Collection<T> rows;

    /** */
    private boolean last;

    /** */
    private boolean fail;

    /** TODO: class of exception. Throwable?*/
    private IgniteException excp;

    /**
     * @param ctx Kernal context.
     * @param nodeId Source.
     * @param res Response.
     */
    public CacheQueryResultPage(final GridKernalContext ctx, UUID nodeId, GridCacheQueryResponse res) {
        this.nodeId = nodeId;
        this.res = res;

        // res == null means that it is a terminating dummy page for the given source node ID.
        if (res != null && res.error() == null) {
            rows = (Collection<T>) res.data();

            last = res.isFinished();

            if (rows != null)
                rowsInPage = rows.size();
            else
                throw new IllegalStateException();
        }
        else {
            rowsInPage = 0;

            rows = Collections.emptyList();
        }
    }

    /**
     * @return {@code true} If this is a dummy fail page.
     */
    public boolean fail() {
        return fail;
    }

    /**
     * @return Exception for failed response.
     */
    public IgniteException exception() {
        return excp;
    }

    /**
     * @return {@code true} If this is either a real last page for a source or
     *      a dummy terminating page with no rows.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return {@code true} If it is a dummy last.
     */
    public boolean dummyLast() {
        return last && res == null;
    }

    /**
     * @param last Last page for a source.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @param e Ignite exception.
     */
    public void fail(IgniteException e) {
        fail = true;
        excp = e;
    }


    /**
     * @return Number on rows in this page.
     */
    public int rowsInPage() {
        return rowsInPage;
    }

    /**
     * @return Rows.
     */
    public Collection<T> rows() {
        Collection<T> r = rows;

        assert r != null;

        rows = null;

        return r;
    }

    /**
     * @return Result source node ID.
     */
    public UUID sourceNodeId() {
        return nodeId;
    }

    /**
     * @return Response.
     */
    public GridCacheQueryResponse response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheQueryResultPage.class, this);
    }
}
