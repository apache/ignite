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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;

import java.util.*;

/**
 * Query cursor implementation.
 */
public class QueryCursorImpl<T> implements QueryCursor<T> {
    /** */
    private GridH2ResultSetIterator<T> iter;

    /** */
    private boolean iterTaken;

    /**
     * @param iter Iterator.
     */
    public QueryCursorImpl(GridH2ResultSetIterator<T> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        if (iter == null)
            throw new IgniteException("Cursor is closed.");

        if (iterTaken)
            throw new IgniteException("Iterator is already taken from this cursor.");

        iterTaken = true;

        return iter;
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        ArrayList<T> all = new ArrayList<>();

        for (T t : this) all.add(t); // Implicitly calls iterator() to do all checks.

        close();

        return all;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        GridH2ResultSetIterator<T> i;

        if ((i = iter) != null) {
            iter = null;

            try {
                i.close();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
