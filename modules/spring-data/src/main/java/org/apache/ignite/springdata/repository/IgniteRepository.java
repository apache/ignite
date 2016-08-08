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
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;

/**
 * Ignite repository. Extend it to use Ignite via spring-data interface.
 * @param <T>
 * @param <ID>
 */
@NoRepositoryBean
public interface IgniteRepository<T, ID extends Serializable> extends CrudRepository<T, ID> {
    /**
     * Save entity to cache with set key.
     * @param key Key.
     * @param entity Entity.
     */
    public <S extends T> void save(ID key, S entity);

    /** Not supported! Use {@link #save(Serializable, Object)} instead!*/
    @Deprecated
    @Override public <S extends T> S save(S entity);

    /** Not supported! Use {@link #save(Serializable, Object)} instead!*/
    @Deprecated
    @Override public <S extends T> Iterable<S> save(Iterable<S> entities);

    /** Not supported! Use {@link #delete(Serializable)} instead!*/
    @Deprecated
    @Override public void delete(T entity);

    /** Not supported! Use {@link #delete(Serializable)} instead!*/
    @Deprecated
    @Override public void delete(Iterable<? extends T> entities);
}