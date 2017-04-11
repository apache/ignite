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
package org.apache.ignite.springdata.repository;

import java.io.Serializable;
import java.util.Map;
import org.springframework.data.repository.CrudRepository;

/**
 * Apache Ignite repository that extends basic capabilities of {@link CrudRepository}.
 */
public interface IgniteRepository<T, ID extends Serializable> extends CrudRepository<T, ID> {
    /**
     * Saves a given entity using provided key.
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Object)} that generates
     * IDs (keys) that are not unique cluster wide.
     *
     * @param key Entity's key.
     * @param entity Entity to save.
     * @param <S> Entity type.
     * @return Saved entity.
     */
    <S extends T> S save(ID key, S entity);

    /**
     * Saves all given keys and entities combinations.
     * </p>
     * It's suggested to use this method instead of default {@link CrudRepository#save(Iterable)} that generates
     * IDs (keys) that are not unique cluster wide.
     *
     * @param entities Map of key-entities pairs to save.
     * @param <S> type of entities.
     * @return Saved entities.
     */
    <S extends T> Iterable<S> save(Map<ID, S> entities);

    /**
     * Deletes all the entities for the provided ids.
     *
     * @param ids List of ids to delete.
     */
    void deleteAll(Iterable<ID> ids);
}
