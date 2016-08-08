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

package org.apache.ignite.springdata.repository.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Realization of IgniteRepository.
 * @param <T>
 * @param <ID>
 */
@NoRepositoryBean
public class IgniteRepositoryImpl<T, ID extends Serializable> implements IgniteRepository<T, ID> {
    /** Cache. */
    private final IgniteCache<ID, T> cache;

    /**
     * @param cache Cache.
     */
    public IgniteRepositoryImpl(IgniteCache cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public <S extends T> S save(S entity) {
        throw new UnsupportedOperationException("Use save(key, entity); instead!");
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public <S extends T> Iterable<S> save(Iterable<S> entities) {
        throw new UnsupportedOperationException("Use save(key, entity); instead!");
    }

    /** {@inheritDoc} */
    @Override public <S extends T> void save(ID key, S entity) {
        cache.put(key, entity);
    }

    /** {@inheritDoc} */
    @Override public T findOne(ID id) {
        return cache.get(id);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(ID id) {
        return cache.containsKey(id);
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll() {
        final Iterator<Cache.Entry<ID, T>> iter = cache.iterator();

        return new Iterable<T>() {
            @Override public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public T next() {
                        return iter.next().getValue();
                    }

                    @Override public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll(Iterable<ID> ids) {
        if (ids instanceof Set)
            return cache.getAll((Set<ID>)ids).values();

        if (ids instanceof Collection)
            return cache.getAll(new HashSet<>((Collection<ID>) ids)).values();

        HashSet<ID> keys = new HashSet<>();

        for (ID id : ids)
            keys.add(id);

        return cache.getAll(keys).values();
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return cache.sizeLong(CachePeekMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override public void delete(ID id) {
        cache.remove(id);
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public void delete(T entity) {
        throw new UnsupportedOperationException("Use delete(id); instead!");
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public void delete(Iterable<? extends T> entities) {
        throw new UnsupportedOperationException("Use delete(id); instead!");
    }

    /** {@inheritDoc} */
    @Override public void deleteAll() {
        cache.clear();
    }
}