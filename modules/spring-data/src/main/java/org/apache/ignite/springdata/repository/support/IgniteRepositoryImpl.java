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
package org.apache.ignite.springdata.repository.support;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.springdata.repository.IgniteRepository;

/**
 * General Apache Ignite repository implementation.
 */
public class IgniteRepositoryImpl<T, ID extends Serializable> implements IgniteRepository<T, ID> {
    /** Ignite Cache bound to the repository */
    private final IgniteCache<ID, T> cache;

    /**
     * Repository constructor.
     *
     * @param cache Initialized cache instance.
     */
    public IgniteRepositoryImpl(IgniteCache<ID, T> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public <S extends T> S save(ID key, S entity) {
        cache.put(key, entity);

        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends T> Iterable<S> save(Map<ID, S> entities) {
        cache.putAll(entities);

        return entities.values();
    }

    /** {@inheritDoc} */
    @Override public <S extends T> S save(S entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(key,value) method instead.");
    }

    /** {@inheritDoc} */
    @Override public <S extends T> Iterable<S> save(Iterable<S> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(Map<keys,value>) method instead.");
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
            return cache.getAll(new HashSet<>((Collection<ID>)ids)).values();

        TreeSet<ID> keys = new TreeSet<>();

        for (ID id : ids)
            keys.add(id);

        return cache.getAll(keys).values();
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return cache.size(CachePeekMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override public void delete(ID id) {
        cache.remove(id);
    }

    /** {@inheritDoc} */
    @Override public void delete(T entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.delete(key) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void delete(Iterable<? extends T> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteAll(keys) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Iterable<ID> ids) {
        if (ids instanceof Set)
            cache.removeAll((Set<ID>)ids);

        if (ids instanceof Collection)
            cache.removeAll(new HashSet<>((Collection<ID>)ids));

        TreeSet<ID> keys = new TreeSet<>();

        for (ID id : ids)
            keys.add(id);

        cache.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll() {
        cache.clear();
    }
}
