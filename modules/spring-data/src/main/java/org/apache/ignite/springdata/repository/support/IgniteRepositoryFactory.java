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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.springdata.IgniteKeyValueAdapter;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.Query;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.apache.ignite.springdata.repository.query.IgniteQuery;
import org.apache.ignite.springdata.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata.repository.query.IgniteRepositoryQuery;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Crucial for spring-data functionality class. Create proxies for repositories.
 */
public class IgniteRepositoryFactory extends KeyValueRepositoryFactory {
    /** Ignite instance */
    private Ignite ignite;

    /** Mapping of a repository to a cache. */
    private final Map<Class<?>, String> repoToCache = new HashMap<>();

    public IgniteRepositoryFactory(KeyValueOperations keyValueOperations) {
        super(keyValueOperations);

        initRepositoryFactory(keyValueOperations);
    }

    public IgniteRepositoryFactory(KeyValueOperations keyValueOperations,
        Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(keyValueOperations, queryCreator);

        initRepositoryFactory(keyValueOperations);
    }

    public IgniteRepositoryFactory(KeyValueOperations keyValueOperations,
        Class<? extends AbstractQueryCreator<?, ?>> queryCreator,
        Class<? extends RepositoryQuery> repositoryQueryType) {
        super(keyValueOperations, queryCreator, repositoryQueryType);

        initRepositoryFactory(keyValueOperations);
    }

    private void initRepositoryFactory(KeyValueOperations keyValueOperations) {
        try {
            Field field = ReflectionUtils.findField(KeyValueTemplate.class,
                new org.springframework.util.ReflectionUtils.FieldFilter() {
                    @Override public boolean matches(Field field) {
                        return field.getName().equals("adapter");
                    }
                });

            field.setAccessible(true);

            IgniteKeyValueAdapter adapter = (IgniteKeyValueAdapter)field.get(keyValueOperations);

            field = ReflectionUtils.findField(IgniteKeyValueAdapter.class,
                new org.springframework.util.ReflectionUtils.FieldFilter() {
                    @Override public boolean matches(Field field) {
                        return field.getName().equals("ignite");
                    }
                });

            field.setAccessible(true);

            ignite = (Ignite)field.get(adapter);

            if (ignite == null)
                throw new IgniteException("Failed to initialize IgniteRepositoryFactory properly: Ignite instance is " +
                    "not set in IgniteKeyValueAdapter");
        }
        catch (Exception e) {
            throw new IgniteException("Failed to initialize IgniteRepositoryFactory properly.", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return SimpleIgniteRepository.class;
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
                        "a query string via org.apache.ignite.springdata.repository.Query annotation.");

                return new IgniteRepositoryQuery(metadata, IgniteQueryGenerator.generateSql(mtd, metadata), mtd,
                    factory, ignite.getOrCreateCache(repoToCache.get(metadata.getRepositoryInterface())));
            }
        };
    }

    /**
     * @param s
     * @return
     */
    private boolean isFieldQuery(String s) {
        return s.matches("^SELECT.*") && !s.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }
}






