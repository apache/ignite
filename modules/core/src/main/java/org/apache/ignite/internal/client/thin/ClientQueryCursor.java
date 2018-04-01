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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientException;
import org.jetbrains.annotations.NotNull;

/**
 * Thin client query cursor.
 */
class ClientQueryCursor<T> implements QueryCursor<T> {
    /** Pager. */
    private final QueryPager<T> pager;

    /** Constructor. */
    ClientQueryCursor(QueryPager<T> pager) {
        this.pager = pager;
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        List<T> res = new ArrayList<>();

        for (T ent : this)
            res.add(ent);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            pager.close();
        }
        catch (Exception ignored) {
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return new Iterator<T>() {
            private Iterator<T> currPageIt = null;

            @Override public boolean hasNext() {
                return pager.hasNext() || (currPageIt != null && currPageIt.hasNext());
            }

            @Override public T next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (currPageIt == null || (!currPageIt.hasNext() && pager.hasNext())) {
                    try {
                        Collection<T> currPage = pager.next();
                        currPageIt = currPage.iterator();
                    }
                    catch (ClientException e) {
                        throw new RuntimeException("Failed to retrieve query results", e);
                    }
                }

                return currPageIt.next();
            }
        };
    }

    /**
     * @return Pager.
     */
    QueryPager<T> getPager() {
        return pager;
    }
}
