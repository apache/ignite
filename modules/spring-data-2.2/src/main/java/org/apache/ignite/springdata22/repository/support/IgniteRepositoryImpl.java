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
package org.apache.ignite.springdata22.repository.support;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.springframework.context.annotation.Conditional;
import org.springframework.lang.Nullable;

/**
 * General Apache Ignite repository implementation. This bean should've never been loaded by context directly, only via
 * {@link IgniteRepositoryFactory}
 *
 * @param <V> the cache value type
 * @param <K> the cache key type
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
@Conditional(ConditionFalse.class)
public class IgniteRepositoryImpl<V, K extends Serializable> implements IgniteRepository<V, K> {
    /**
     * Ignite Cache bound to the repository
     */
    private final IgniteCache<K, V> cache;

    /**
     * Ignite instance bound to the repository
     */
    private final Ignite ignite;

    /**
     * Repository constructor.
     *
     * @param ignite the ignite
     * @param cache  Initialized cache instance.
     */
    public IgniteRepositoryImpl(Ignite ignite, IgniteCache<K, V> cache) {
        this.cache = cache;
        this.ignite = ignite;
    }

    /**
     * {@inheritDoc} @return the ignite cache
     */
    @Override public IgniteCache<K, V> cache() {
        return cache;
    }

    /**
     * {@inheritDoc} @return the ignite
     */
    @Override public Ignite ignite() {
        return ignite;
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param key    the key
     * @param entity the entity
     * @return the s
     */
    @Override public <S extends V> S save(K key, S entity) {
        cache.put(key, entity);

        return entity;
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param entities the entities
     * @return the iterable
     */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities) {
        cache.putAll(entities);

        return entities.values();
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param key       the key
     * @param entity    the entity
     * @param expiryPlc the expiry policy
     * @return the s
     */
    @Override public <S extends V> S save(K key, S entity, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).put(key, entity);
        else
            cache.put(key, entity);
        return entity;
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param entities  the entities
     * @param expiryPlc the expiry policy
     * @return the iterable
     */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).putAll(entities);
        else
            cache.putAll(entities);
        return entities.values();
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param entity the entity
     * @return the s
     */
    @Override public <S extends V> S save(S entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(key,value) method instead.");
    }

    /**
     * {@inheritDoc} @param <S>  the type parameter
     *
     * @param entities the entities
     * @return the iterable
     */
    @Override public <S extends V> Iterable<S> saveAll(Iterable<S> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(Map<keys,value>) method instead.");
    }

    /**
     * {@inheritDoc} @param id the id
     *
     * @return the optional
     */
    @Override public Optional<V> findById(K id) {
        return Optional.ofNullable(cache.get(id));
    }

    /**
     * {@inheritDoc} @param id the id
     *
     * @return the boolean
     */
    @Override public boolean existsById(K id) {
        return cache.containsKey(id);
    }

    /**
     * {@inheritDoc} @return the iterable
     */
    @Override public Iterable<V> findAll() {
        final Iterator<Cache.Entry<K, V>> iter = cache.iterator();

        return new Iterable<V>() {
            @Override public Iterator<V> iterator() {
                return new Iterator<V>() {
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public V next() {
                        return iter.next().getValue();
                    }

                    @Override public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    /**
     * {@inheritDoc} @param ids the ids
     *
     * @return the iterable
     */
    @Override public Iterable<V> findAllById(Iterable<K> ids) {
        if (ids instanceof Set)
            return cache.getAll((Set<K>)ids).values();

        if (ids instanceof Collection)
            return cache.getAll(new HashSet<>((Collection<K>)ids)).values();

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        return cache.getAll(keys).values();
    }

    /**
     * {@inheritDoc} @return the long
     */
    @Override public long count() {
        return cache.size(CachePeekMode.PRIMARY);
    }

    /**
     * {@inheritDoc} @param id the id
     */
    @Override public void deleteById(K id) {
        cache.remove(id);
    }

    /**
     * {@inheritDoc} @param entity the entity
     */
    @Override public void delete(V entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteById(key) method instead.");
    }

    /**
     * {@inheritDoc} @param entities the entities
     */
    @Override public void deleteAll(Iterable<? extends V> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteAllById(keys) method instead.");
    }

    /**
     * {@inheritDoc} @param ids the ids
     */
    @Override public void deleteAllById(Iterable<K> ids) {
        if (ids instanceof Set) {
            cache.removeAll((Set<K>)ids);
            return;
        }

        if (ids instanceof Collection) {
            cache.removeAll(new HashSet<>((Collection<K>)ids));
            return;
        }

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        cache.removeAll(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void deleteAll() {
        cache.clear();
    }

}
