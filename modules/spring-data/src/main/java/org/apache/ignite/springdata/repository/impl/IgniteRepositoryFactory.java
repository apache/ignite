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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.config.Query;
import org.apache.ignite.springdata.config.RepositoryConfig;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Crucial for spring-data functionality class. Create proxies for repositories.
 */
public class IgniteRepositoryFactory extends RepositoryFactorySupport {
    /** Ignite. */
    private Ignite ignite;

    /** Cache name for repository. */
    private final Map<Class<?>, String> cacheNameForRepo = new HashMap<>();

    /**
     * @param ignite Ignite.
     */
    public IgniteRepositoryFactory(Ignite ignite) {
        this.ignite = ignite;
    }


    /** {@inheritDoc} */
    @Override public <T, ID extends Serializable> EntityInformation<T, ID> getEntityInformation(Class<T> domainCls) {
        return new AbstractEntityInformation<T, ID>(domainCls) {
            @Override public ID getId(T entity) {
                return null;
            }

            @Override public Class<ID> getIdType() {
                return null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Object getTargetRepository(RepositoryInformation metadata) {
        return getTargetRepositoryViaReflection(metadata,
            ignite.getOrCreateCache(cacheNameForRepo.get(metadata.getRepositoryInterface())));
    }

    /** {@inheritDoc} */
    @Override protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return IgniteRepositoryImpl.class;
    }

    /** {@inheritDoc} */
    @Override protected RepositoryMetadata getRepositoryMetadata(Class<?> repoItf) {
        Assert.notNull(repoItf, "Repository interface must not be null!");
        Assert.isAssignable(IgniteRepository.class, repoItf, "You should extend IgniteRepository in your own repo.");

        RepositoryConfig annotation = repoItf.getAnnotation(RepositoryConfig.class);
        Assert.notNull(annotation, "You should provide cache name in @RepositoryConfig annotation");
        Assert.hasText(annotation.cacheName());

        cacheNameForRepo.put(repoItf, annotation.cacheName());

        return super.getRepositoryMetadata(repoItf);
    }

    /** {@inheritDoc} */
    @Override protected QueryLookupStrategy getQueryLookupStrategy(final QueryLookupStrategy.Key key,
        EvaluationContextProvider evaluationCtxProvider) {

        return new QueryLookupStrategy() {
            @Override public RepositoryQuery resolveQuery(final Method mtd, final RepositoryMetadata metadata,
                final ProjectionFactory factory, NamedQueries namedQueries) {

                final Query annotation = mtd.getAnnotation(Query.class);

                if (annotation != null) {
                    String qryStr = annotation.value();

                    if (key != Key.CREATE && StringUtils.hasText(qryStr))
                        return new IgniteRepositoryQuery(metadata,
                            new IgniteQuery(
                                qryStr, isFieldQuery(qryStr), IgniteQueryGenerator.isDynamic(mtd)), mtd, factory,
                                ignite.getOrCreateCache(cacheNameForRepo.get(metadata.getRepositoryInterface())));
                }

                if (key == Key.USE_DECLARED_QUERY)
                    throw new IllegalStateException("Query not found! \n" +
                        "\tWith QueryLookupStrategy.Key.USE_DECLARED_QUERY use should use @Query annotation. " +
                        "\nNamed-queries is not supported.");

                return new IgniteRepositoryQuery(
                    metadata,
                    IgniteQueryGenerator.generateSql(mtd, metadata),
                    mtd,
                    factory,
                    ignite.getOrCreateCache(cacheNameForRepo.get(metadata.getRepositoryInterface())));
            }
        };
    }

    /**
     * For SqlQuery we only support SELECT * and SELECT alias.*
     */
    private boolean isFieldQuery(String s) {
        return s.matches("^SELECT.*") && !s.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }
}