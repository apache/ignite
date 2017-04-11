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
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

/**
 * Apache Ignite repository factory bean.
 *
 * @param <T> Repository type, {@link IgniteRepository}
 * @param <S> Domain object class.
 * @param <ID> Domain object key, super expects {@link Serializable}.
 */
public class IgniteRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
    extends KeyValueRepositoryFactoryBean<T, S, ID> {

    /**
     * Creates a new {@code IgniteRepositoryFactoryBean} for the given repository interface.
     *
     * @param repositoryInterface must not be {@literal null}.
     */
    public IgniteRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    /** {@inheritDoc} */
    @Override protected KeyValueRepositoryFactory createRepositoryFactory(KeyValueOperations operations,
        Class<? extends AbstractQueryCreator<?, ?>> queryCreator,
        Class<? extends RepositoryQuery> repositoryQueryType) {
        return new IgniteRepositoryFactory(operations, queryCreator, repositoryQueryType);
    }
}

