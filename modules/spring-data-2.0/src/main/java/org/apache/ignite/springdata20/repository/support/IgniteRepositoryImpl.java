/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.springdata20.repository.support;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.springdata20.repository.IgniteRepository;
import org.springframework.context.annotation.Conditional;

/**
 * General Apache Ignite repository implementation.
 * This bean should've never been loaded by context directly, only via {@link IgniteRepositoryFactory}
 */
@Conditional(ConditionFalse.class)
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
    @Override public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(Map<keys,value>) method instead.");
    }

    /** {@inheritDoc} */
    @Override public Optional<T> findById(ID id) {
        return Optional.ofNullable(cache.get(id));
    }

    /** {@inheritDoc} */
    @Override public boolean existsById(ID id) {
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
    @Override public Iterable<T> findAllById(Iterable<ID> ids) {
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
    @Override public void deleteById(ID id) {
        cache.remove(id);
    }

    /** {@inheritDoc} */
    @Override public void delete(T entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteById(key) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Iterable<? extends T> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteAllById(keys) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAllById(Iterable<ID> ids) {
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
