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
import java.util.Map;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.util.Assert;

/**
 * General Apache Ignite repository implementation.
 */
public class SimpleIgniteRepository<T, ID extends Serializable>
    extends SimpleKeyValueRepository<T, ID> implements IgniteRepository<T, ID> {
    /** */
    private final KeyValueOperations operations;

    /**
     * Creates a new {@link SimpleKeyValueRepository} for the given {@link EntityInformation} and {@link
     * KeyValueOperations}.
     *
     * @param metadata must not be {@literal null}.
     * @param operations must not be {@literal null}.
     */
    public SimpleIgniteRepository(EntityInformation<T, ID> metadata, KeyValueOperations operations) {
        super(metadata, operations);

        this.operations = operations;
    }

    /** {@inheritDoc} */
    @Override public <S extends T> S save(ID key, S entity) {
        Assert.notNull(entity, "Entity must not be null!");

        operations.update(key, entity);

        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends T> Iterable<S> save(Map<ID, S> entities) {
        for (ID key : entities.keySet())
            operations.update(key, entities.get(key));

        return entities.values();
    }
}
