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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.query.QueryCursor;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

/**
 * SqlQuery key-value iterable.
 */
public class QueryKeyValueIterable<K, V> implements Iterable<Cache.Entry<K, V>> {
    /** Underlying fields query cursor. */
    private final QueryCursor<List<?>> cur;

    /**
     * Constructor.
     *
     * @param cur Underlying fields query cursor.
     */
    public QueryKeyValueIterable(QueryCursor<List<?>> cur) {
        this.cur = cur;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        return new QueryKeyValueIterator<>(cur.iterator());
    }

    /**
     * @return Underlying fields query cursor.
     */
    QueryCursor<List<?>> cursor() {
        return cur;
    }
}
