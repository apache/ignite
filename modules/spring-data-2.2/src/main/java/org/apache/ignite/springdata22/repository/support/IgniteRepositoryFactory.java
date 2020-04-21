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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.apache.ignite.springdata22.repository.config.Query;
import org.apache.ignite.springdata22.repository.config.RepositoryConfig;
import org.apache.ignite.springdata22.repository.query.IgniteQuery;
import org.apache.ignite.springdata22.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata22.repository.query.IgniteRepositoryQuery;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Crucial for spring-data functionality class. Create proxies for repositories.
 */
public class IgniteRepositoryFactory extends RepositoryFactorySupport {
    /** Ignite instance */
    private Ignite ignite;

    /** Spring application context */
    private ApplicationContext ctx;

    /** Spring application bean factory */
    private DefaultListableBeanFactory beanFactory;

    /** Spring application expression resolver */
    private StandardBeanExpressionResolver resolver = new StandardBeanExpressionResolver();

    /** Spring application bean expression context */
    private BeanExpressionContext beanExpressionContext;

    /** Mapping of a repository to a cache. */
    private final Map<Class<?>, String> repoToCache = new HashMap<>();

    /**
     * Creates the factory with initialized {@link Ignite} instance.
     *
     * @param ignite
     */
    public IgniteRepositoryFactory(Ignite ignite, ApplicationContext ctx) {
        this.ignite = ignite;

        this.ctx = ctx;

        this.beanFactory = new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory());

        this.beanExpressionContext = new BeanExpressionContext(beanFactory, null);
    }

    /**
     * Initializes the factory with provided {@link IgniteConfiguration} that is used to start up an underlying
     * {@link Ignite} instance.
     *
     * @param cfg Ignite configuration.
     */
    public IgniteRepositoryFactory(IgniteConfiguration cfg, ApplicationContext ctx) {
        this.ignite = Ignition.start(cfg);

        this.ctx = ctx;

        this.beanFactory = new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory());

        this.beanExpressionContext = new BeanExpressionContext(beanFactory, null);
    }

    /**
     * Initializes the factory with provided a configuration under {@code springCfgPath} that is used to start up
     * an underlying {@link Ignite} instance.
     *
     * @param springCfgPath A path to Ignite configuration.
     */
    public IgniteRepositoryFactory(String springCfgPath, ApplicationContext ctx) {
        this.ignite = Ignition.start(springCfgPath);

        this.ctx = ctx;

        this.beanFactory = new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory());

        this.beanExpressionContext = new BeanExpressionContext(beanFactory, null);
    }

    /** {@inheritDoc} */
    @Override public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
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

        String cacheName = evaluateExpression(annotation.cacheName());

        repoToCache.put(repoItf, cacheName);

        return super.getRepositoryMetadata(repoItf);
    }

    /**
     *  evaluate the SpEL expression
     *
     * @param spelExpression SpEL expression
     * @return the result of execution of the SpEL expression
     */
    @NotNull private String evaluateExpression(String spelExpression) {
        return (String)resolver.evaluate(spelExpression, beanExpressionContext);
    }

    /** {@inheritDoc} */
    @Override protected Object getTargetRepository(RepositoryInformation metadata) {
        return getTargetRepositoryViaReflection(metadata,
            ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
    }

    /** {@inheritDoc} */
    @Override protected Optional<QueryLookupStrategy> getQueryLookupStrategy(final QueryLookupStrategy.Key key,
        QueryMethodEvaluationContextProvider evaluationCtxProvider) {
        return Optional.of((mtd, metadata, factory, namedQueries) -> {

            final Query annotation = mtd.getAnnotation(Query.class);

            if (annotation != null) {
                String qryStr = annotation.value();

                if (key != QueryLookupStrategy.Key.CREATE && StringUtils.hasText(qryStr)) {
                    return new IgniteRepositoryQuery(metadata,
                            new IgniteQuery(qryStr, isFieldQuery(qryStr), IgniteQueryGenerator.getOptions(mtd)),
                            mtd, factory, ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
                }
            }

            if (key == QueryLookupStrategy.Key.USE_DECLARED_QUERY) {
                throw new IllegalStateException("To use QueryLookupStrategy.Key.USE_DECLARED_QUERY, pass " +
                        "a query string via org.apache.ignite.springdata.repository.config.Query annotation.");
            }

            return new IgniteRepositoryQuery(metadata, IgniteQueryGenerator.generateSql(mtd, metadata), mtd,
                factory, ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
        });
    }

    /**
     * @param qry Query string.
     * @return {@code true} if query is SqlFieldsQuery.
     */
    private boolean isFieldQuery(String qry) {
        String qryUpperCase = qry.toUpperCase();

        return isStatement(qryUpperCase) && !qryUpperCase.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }

    /**
     * Evaluates if the query starts with a clause.<br>
     * <code>SELECT, INSERT, UPDATE, MERGE, DELETE</code>
     *
     * @param qryUpperCase Query string.
     * @return {@code true} if query is full SQL statement.
     */
    private boolean isStatement(String qryUpperCase ) {
        return qryUpperCase.matches("^\\s*SELECT\\b.*") || qryUpperCase.matches("^\\s*UPDATE\\b.*") || qryUpperCase.matches("^\\s*DELETE\\b.*") ||
            qryUpperCase.matches("^\\s*MERGE\\b.*") || qryUpperCase.matches("^\\s*INSERT\\b.*");
    }
}
