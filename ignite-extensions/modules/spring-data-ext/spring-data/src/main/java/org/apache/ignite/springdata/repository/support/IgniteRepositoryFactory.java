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

import java.util.Optional;
import org.apache.ignite.springdata.proxy.IgniteCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.repository.config.DynamicQueryConfig;
import org.apache.ignite.springdata.repository.config.Query;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.apache.ignite.springdata.repository.query.IgniteQuery;
import org.apache.ignite.springdata.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata.repository.query.IgniteRepositoryQuery;
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
    /** Spring application expression resolver */
    private final StandardBeanExpressionResolver resolver = new StandardBeanExpressionResolver();

    /** Spring application bean expression context */
    private final BeanExpressionContext beanExpressionContext;

    /** Ignite cache proxy instance associated with the current repository. */
    private final IgniteCacheProxy<?, ?> cache;

    /** Ignite proxy instance associated with the current repository. */
    private final IgniteProxy ignite;

    /**
     * @param ctx Spring Application context.
     * @param repoInterface Repository interface.
     */
    public IgniteRepositoryFactory(ApplicationContext ctx, Class<?> repoInterface) {
        ignite = ctx.getBean(IgniteProxy.class, repoInterface);

        beanExpressionContext = new BeanExpressionContext(
            new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory()),
            null);

        RepositoryConfig cfg = getRepositoryConfiguration(repoInterface);

        String cacheName = evaluateExpression(cfg.cacheName());

        Assert.hasText(cacheName, "Invalid configuration for repository " + repoInterface.getName() +
            ". Set a name of an Apache Ignite cache using " + RepositoryConfig.class.getName() +
            " annotation to map this repository to the underlying cache.");

        cache = cfg.autoCreateCache() ? ignite.getOrCreateCache(cacheName) : ignite.cache(cacheName);

        if (cache == null) {
            throw new IllegalArgumentException(
                "Cache '" + cacheName + "' not found for repository interface " + repoInterface.getName()
                    + ". Please, add a cache configuration to ignite configuration"
                    + " or pass autoCreateCache=true to " + RepositoryConfig.class.getName() + " annotation.");
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

    /**
     * Evaluate the SpEL expression
     *
     * @param spelExpression SpEL expression
     * @return the result of execution of the SpEL expression
     */
    private String evaluateExpression(String spelExpression) {
        return (String)resolver.evaluate(spelExpression, beanExpressionContext);
    }

    /** {@inheritDoc} */
    @Override protected Object getTargetRepository(RepositoryInformation metadata) {
        return getTargetRepositoryViaReflection(metadata, ignite, cache);
    }

    /** {@inheritDoc} */
    @Override protected Optional<QueryLookupStrategy> getQueryLookupStrategy(final QueryLookupStrategy.Key key,
        QueryMethodEvaluationContextProvider evaluationContextProvider) {
        return Optional.of((mtd, metadata, factory, namedQueries) -> {
            final Query annotation = mtd.getAnnotation(Query.class);
            if (annotation != null && (StringUtils.hasText(annotation.value()) || annotation.textQuery() || annotation
                .dynamicQuery())) {

                String qryStr = annotation.value();

                boolean annotatedIgniteQuery = !annotation.dynamicQuery() && (StringUtils.hasText(qryStr) || annotation
                    .textQuery());

                IgniteQuery query = annotatedIgniteQuery
                    ? new IgniteQuery(
                        qryStr,
                        !annotation.textQuery() && (isFieldQuery(qryStr) || annotation.forceFieldsQuery()),
                        annotation.textQuery(),
                        false,
                        true,
                        IgniteQueryGenerator.getOptions(mtd))
                    : null;

                if (key != QueryLookupStrategy.Key.CREATE) {
                    return new IgniteRepositoryQuery(metadata, query, mtd, factory, cache,
                        annotatedIgniteQuery ? DynamicQueryConfig.fromQueryAnnotation(annotation) : null,
                        evaluationContextProvider);
                }
            }

            if (key == QueryLookupStrategy.Key.USE_DECLARED_QUERY) {
                throw new IllegalStateException("To use QueryLookupStrategy.Key.USE_DECLARED_QUERY, pass "
                    + "a query string via org.apache.ignite.springdata.repository"
                    + ".config.Query annotation.");
            }

            return new IgniteRepositoryQuery(metadata, IgniteQueryGenerator.generateSql(mtd, metadata), mtd, factory,
                cache, DynamicQueryConfig.fromQueryAnnotation(annotation), evaluationContextProvider);
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

    /**
     * @return Configuration of the specified repository.
     * @throws IllegalArgumentException If no configuration is specified.
     * @see RepositoryConfig
     */
    static RepositoryConfig getRepositoryConfiguration(Class<?> repoInterface) {
        RepositoryConfig cfg = repoInterface.getAnnotation(RepositoryConfig.class);

        Assert.notNull(cfg, "Invalid configuration for repository " + repoInterface.getName() + ". " +
            RepositoryConfig.class.getName() + " annotation must be specified for each repository interface.");

        return cfg;
    }
}
