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
package org.apache.ignite.springdata20.repository;

import java.io.Serializable;
import java.util.Map;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.repository.CrudRepository;

/**
 * Apache Ignite repository that extends basic capabilities of {@link CrudRepository}.
 *
 * @param <V> the cache value type
 * @param <K> the cache key type
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
public interface IgniteRepository<V, K extends Serializable> extends CrudRepository<V, K> {
    /**
     * Returns the Ignite instance bound to the repository
     *
     * @return the Ignite instance bound to the repository
     */
    public Ignite ignite();

    /**
     * Returns the Ignite Cache bound to the repository
     *
     * @return the Ignite Cache bound to the repository
     */
    public IgniteCache<K, V> cache();

    /**
     * Saves a given entity using provided key.
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Object)} that generates IDs
     * (keys) that are not unique cluster wide.
     *
     * @param <S>    Entity type.
     * @param key    Entity's key.
     * @param entity Entity to save.
     * @return Saved entity.
     */
    public <S extends V> S save(K key, S entity);

    /**
     * Saves all given keys and entities combinations.
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Object)} that generates IDs
     * (keys) that are not unique cluster wide.
     *
     * @param <S>      Type of entities.
     * @param entities Map of key-entities pairs to save.
     * @return Saved entities.
     */
    public <S extends V> Iterable<S> save(Map<K, S> entities);

    /**
     * Saves a given entity using provided key with expiry policy
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Object)} that generates IDs
     * (keys) that are not unique cluster wide.
     *
     * @param <S>       Entity type.
     * @param key       Entity's key.
     * @param entity    Entity to save.
     * @param expiryPlc ExpiryPolicy to apply, if not null.
     * @return Saved entity.
     */
    public <S extends V> S save(K key, S entity, @Nullable ExpiryPolicy expiryPlc);

    /**
     * Saves all given keys and entities combinations with expiry policy
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Object)} that generates IDs
     * (keys) that are not unique cluster wide.
     *
     * @param <S>       Type of entities.
     * @param entities  Map of key-entities pairs to save.
     * @param expiryPlc ExpiryPolicy to apply, if not null.
     * @return Saved entities.
     */
    public <S extends V> Iterable<S> save(Map<K, S> entities, @Nullable ExpiryPolicy expiryPlc);

    /**
     * Deletes all the entities for the provided ids.
     *
     * @param ids List of ids to delete.
     */
    public void deleteAllById(Iterable<K> ids);

}
