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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.apache.ignite.springdata22.repository.config.DynamicQueryConfig;
import org.apache.ignite.springdata22.repository.config.Query;
import org.apache.ignite.springdata22.repository.config.RepositoryConfig;
import org.apache.ignite.springdata22.repository.query.IgniteQuery;
import org.apache.ignite.springdata22.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata22.repository.query.IgniteRepositoryQuery;
import org.springframework.beans.BeansException;
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
 * <p>
 * Supports multiple Ignite Instances on same JVM.
 * <p>
 * This is pretty useful working with Spring repositories bound to different Ignite intances within same application.
 *
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
public class IgniteRepositoryFactory extends RepositoryFactorySupport {
    /** Spring application context */
    private final ApplicationContext ctx;

    /** Spring application bean factory */
    private final DefaultListableBeanFactory beanFactory;

    /** Spring application expression resolver */
    private final StandardBeanExpressionResolver resolver = new StandardBeanExpressionResolver();

    /** Spring application bean expression context */
    private final BeanExpressionContext beanExpressionContext;

    /** Mapping of a repository to a cache. */
    private final Map<Class<?>, String> repoToCache = new HashMap<>();

    /** Mapping of a repository to a ignite instance. */
    private final Map<Class<?>, Ignite> repoToIgnite = new HashMap<>();

    /**
     * Creates the factory with initialized {@link Ignite} instance.
     *
     * @param ctx the ctx
     */
    public IgniteRepositoryFactory(ApplicationContext ctx) {
        this.ctx = ctx;

        beanFactory = new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory());

        beanExpressionContext = new BeanExpressionContext(beanFactory, null);
    }

    /** */
    private Ignite igniteForRepoConfig(RepositoryConfig config) {
        try {
            String igniteInstanceName = evaluateExpression(config.igniteInstance());
            return (Ignite)ctx.getBean(igniteInstanceName);
        }
        catch (BeansException ex) {
            try {
                String igniteConfigName = evaluateExpression(config.igniteCfg());
                IgniteConfiguration cfg = (IgniteConfiguration)ctx.getBean(igniteConfigName);
                try {
                    // first try to attach to existing ignite instance
                    return Ignition.ignite(cfg.getIgniteInstanceName());
                }
                catch (Exception ignored) {
                    // nop
                }
                return Ignition.start(cfg);
            }
            catch (BeansException ex2) {
                try {
                    String igniteSpringCfgPath = evaluateExpression(config.igniteSpringCfgPath());
                    String path = (String)ctx.getBean(igniteSpringCfgPath);
                    return Ignition.start(path);
                }
                catch (BeansException ex3) {
                    throw new IgniteException("Failed to initialize Ignite repository factory. Ignite instance or"
                        + " IgniteConfiguration or a path to Ignite's spring XML "
                        + "configuration must be defined in the"
                        + " application configuration");
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        return new AbstractEntityInformation<T, ID>(domainClass) {
            /** {@inheritDoc} */
            @Override public ID getId(T entity) {
                return null;
            }

            /** {@inheritDoc} */
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
    @Override protected synchronized RepositoryMetadata getRepositoryMetadata(Class<?> repoItf) {
        Assert.notNull(repoItf, "Repository interface must be set.");
        Assert.isAssignable(IgniteRepository.class, repoItf, "Repository must implement IgniteRepository interface.");

        RepositoryConfig annotation = repoItf.getAnnotation(RepositoryConfig.class);

        Assert.notNull(annotation, "Set a name of an Apache Ignite cache using @RepositoryConfig annotation to map "
            + "this repository to the underlying cache.");

        Assert.hasText(annotation.cacheName(), "Set a name of an Apache Ignite cache using @RepositoryConfig "
            + "annotation to map this repository to the underlying cache.");

        String cacheName = evaluateExpression(annotation.cacheName());

        repoToCache.put(repoItf, cacheName);

        repoToIgnite.put(repoItf, igniteForRepoConfig(annotation));

        return super.getRepositoryMetadata(repoItf);
    }

    /**
     * Evaluate the SpEL expression
     *
     * @param spelExpression SpEL expression
     * @return the result of execution of the SpEL expression
     */
    private String evaluateExpression(String spelExpression) {
        return (String)resolver.evaluate(spelExpression, beanExpressionContext);
    }

    /** Control underlying cache creation to avoid cache creation by mistake */
    private IgniteCache getRepositoryCache(Class<?> repoIf) {
        Ignite ignite = repoToIgnite.get(repoIf);

        RepositoryConfig config = repoIf.getAnnotation(RepositoryConfig.class);

        String cacheName = repoToCache.get(repoIf);

        IgniteCache c = config.autoCreateCache() ? ignite.getOrCreateCache(cacheName) : ignite.cache(cacheName);

        if (c == null) {
            throw new IllegalStateException(
                "Cache '" + cacheName + "' not found for repository interface " + repoIf.getName()
                    + ". Please, add a cache configuration to ignite configuration"
                    + " or pass autoCreateCache=true to org.apache.ignite.springdata22"
                    + ".repository.config.RepositoryConfig annotation.");
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override protected Object getTargetRepository(RepositoryInformation metadata) {
        Ignite ignite = repoToIgnite.get(metadata.getRepositoryInterface());

        return getTargetRepositoryViaReflection(metadata, ignite,
            getRepositoryCache(metadata.getRepositoryInterface()));
    }

    /** {@inheritDoc} */
    @Override protected Optional<QueryLookupStrategy> getQueryLookupStrategy(final QueryLookupStrategy.Key key,
        QueryMethodEvaluationContextProvider evaluationContextProvider) {
        return Optional.of((mtd, metadata, factory, namedQueries) -> {
            final Query annotation = mtd.getAnnotation(Query.class);
            final Ignite ignite = repoToIgnite.get(metadata.getRepositoryInterface());

            if (annotation != null && (StringUtils.hasText(annotation.value()) || annotation.textQuery() || annotation
                .dynamicQuery())) {

                String qryStr = annotation.value();

                boolean annotatedIgniteQuery = !annotation.dynamicQuery() && (StringUtils.hasText(qryStr) || annotation
                    .textQuery());

                IgniteQuery query = annotatedIgniteQuery ? new IgniteQuery(qryStr,
                    !annotation.textQuery() && (isFieldQuery(qryStr) || annotation.forceFieldsQuery()),
                    annotation.textQuery(), false, IgniteQueryGenerator.getOptions(mtd)) : null;

                if (key != QueryLookupStrategy.Key.CREATE) {
                    return new IgniteRepositoryQuery(ignite, metadata, query, mtd, factory,
                        getRepositoryCache(metadata.getRepositoryInterface()),
                        annotatedIgniteQuery ? DynamicQueryConfig.fromQueryAnnotation(annotation) : null,
                        evaluationContextProvider);
                }
            }

            if (key == QueryLookupStrategy.Key.USE_DECLARED_QUERY) {
                throw new IllegalStateException("To use QueryLookupStrategy.Key.USE_DECLARED_QUERY, pass "
                    + "a query string via org.apache.ignite.springdata22.repository"
                    + ".config.Query annotation.");
            }

            return new IgniteRepositoryQuery(ignite, metadata, IgniteQueryGenerator.generateSql(mtd, metadata), mtd,
                factory, getRepositoryCache(metadata.getRepositoryInterface()),
                DynamicQueryConfig.fromQueryAnnotation(annotation), evaluationContextProvider);
        });
    }

    /**
     * @param qry Query string.
     * @return {@code true} if query is SqlFieldsQuery.
     */
    public static boolean isFieldQuery(String qry) {
        String qryUpperCase = qry.toUpperCase();

        return isStatement(qryUpperCase) && !qryUpperCase.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }

    /**
     * Evaluates if the query starts with a clause.<br>
     * <code>SELECT, INSERT, UPDATE, MERGE, DELETE</code>
     *
     * @param qryUpperCase Query string in upper case.
     * @return {@code true} if query is full SQL statement.
     */
    private static boolean isStatement(String qryUpperCase) {
        return qryUpperCase.matches("^\\s*SELECT\\b.*") ||
            // update
            qryUpperCase.matches("^\\s*UPDATE\\b.*") ||
            // delete
            qryUpperCase.matches("^\\s*DELETE\\b.*") ||
            // merge
            qryUpperCase.matches("^\\s*MERGE\\b.*") ||
            // insert
            qryUpperCase.matches("^\\s*INSERT\\b.*");
    }
}
