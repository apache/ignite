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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.config.Query;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.apache.ignite.springdata.repository.query.IgniteQuery;
import org.apache.ignite.springdata.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata.repository.query.IgniteRepositoryQuery;
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
    /** Ignite instance */
    private Ignite ignite;

    /** Mapping of a repository to a cache. */
    private final Map<Class<?>, String> repoToCache = new HashMap<>();

    /**
     * Creates the factory with initialized {@link Ignite} instance.
     *
     * @param ignite
     */
    public IgniteRepositoryFactory(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Initializes the factory with provided {@link IgniteConfiguration} that is used to start up an underlying
     * {@link Ignite} instance.
     *
     * @param cfg Ignite configuration.
     */
    public IgniteRepositoryFactory(IgniteConfiguration cfg) {
        this.ignite = Ignition.start(cfg);
    }

    /**
     * Initializes the factory with provided a configuration under {@code springCfgPath} that is used to start up
     * an underlying {@link Ignite} instance.
     *
     * @param springCfgPath A path to Ignite configuration.
     */
    public IgniteRepositoryFactory(String springCfgPath) {
        this.ignite = Ignition.start(springCfgPath);
    }

    /** {@inheritDoc} */
    @Override public <T, ID extends Serializable> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        return new AbstractEntityInformation<T, ID>(domainClass) {
            @Override public ID getId(T entity) {
                return null;
            }

            @Override public Class<ID> getIdType() {
                return null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return IgniteRepositoryImpl.class;
    }

    /** {@inheritDoc} */
    @Override protected RepositoryMetadata getRepositoryMetadata(Class<?> repoItf) {
        Assert.notNull(repoItf, "Repository interface must be set.");
        Assert.isAssignable(IgniteRepository.class, repoItf, "Repository must implement IgniteRepository interface.");

        RepositoryConfig annotation = repoItf.getAnnotation(RepositoryConfig.class);

        Assert.notNull(annotation, "Set a name of an Apache Ignite cache using @RepositoryConfig annotation to map " +
            "this repository to the underlying cache.");

        Assert.hasText(annotation.cacheName(), "Set a name of an Apache Ignite cache using @RepositoryConfig " +
            "annotation to map this repository to the underlying cache.");

        repoToCache.put(repoItf, annotation.cacheName());

        return super.getRepositoryMetadata(repoItf);
    }

    /** {@inheritDoc} */
    @Override protected Object getTargetRepository(RepositoryInformation metadata) {
        return getTargetRepositoryViaReflection(metadata,
            ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
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
                            new IgniteQuery(qryStr, isFieldQuery(qryStr), IgniteQueryGenerator.getOptions(mtd)),
                            mtd, factory, ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
                }

                if (key == QueryLookupStrategy.Key.USE_DECLARED_QUERY)
                    throw new IllegalStateException("To use QueryLookupStrategy.Key.USE_DECLARED_QUERY, pass " +
                        "a query string via org.apache.ignite.springdata.repository.config.Query annotation.");

                return new IgniteRepositoryQuery(metadata, IgniteQueryGenerator.generateSql(mtd, metadata), mtd,
                    factory, ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
            }
        };
    }

    /**
     * @param qry Query string.
     * @return {@code true} if query is SQLFieldsQuery.
     */
    private boolean isFieldQuery(String qry) {
        return qry.matches("^SELECT.*") && !qry.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }
}






