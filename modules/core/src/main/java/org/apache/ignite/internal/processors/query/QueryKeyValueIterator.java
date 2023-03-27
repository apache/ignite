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

import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;

/**
 * SqlQuery key-value iterator.
 */
public class QueryKeyValueIterator<K, V> implements Iterator<Cache.Entry<K, V>> {
    /** Target iterator. */
    private final Iterator<List<?>> iter;

    /**
     * Constructor.
     *
     * @param iter Target iterator.
     */
    public QueryKeyValueIterator(Iterator<List<?>> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Cache.Entry<K, V> next() {
        try {
            List<?> row = iter.next();

            return new CacheEntryImpl<>((K)row.get(0), (V)row.get(1));
        }
        catch (CacheException e) {
            throw e;
        }
        catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
