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

package org.springframework.data.ignite.repository.impl;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.ignite.repository.IgniteRepository;
import org.springframework.data.ignite.repository.config.Query;
import org.springframework.data.ignite.repository.config.RepositoryConfig;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 *
 *
 */
public class IgniteRepositoryFactory extends RepositoryFactorySupport {
    private Ignite ignite;

    private final Map<Class<?>, String> cacheNameForRepos = new HashMap<>();

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
            ignite.getOrCreateCache(cacheNameForRepos.get(metadata.getRepositoryInterface())));
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

        cacheNameForRepos.put(repoItf, annotation.cacheName());

        return super.getRepositoryMetadata(repoItf);
    }

    /** {@inheritDoc} */
    @Override protected QueryLookupStrategy getQueryLookupStrategy(QueryLookupStrategy.Key key,
        EvaluationContextProvider evaluationCtxProvider) {

        return new QueryLookupStrategy() {
            @Override public RepositoryQuery resolveQuery(final Method mtd, final RepositoryMetadata metadata,
                final ProjectionFactory factory, NamedQueries namedQueries) {

                final Query annotation = mtd.getAnnotation(Query.class);

                if (annotation != null && StringUtils.hasText(annotation.value()))
                    return new IgniteRepositoryQuery(metadata,
                        new IgniteQuery(annotation.value(), false, IgniteQueryGenerator.isDynamic(mtd)), mtd, factory);

                //TODO namedQueries handling

                return new IgniteRepositoryQuery(
                    metadata,
                    IgniteQueryGenerator.generateSql(mtd, metadata),
                    mtd,
                    factory);
            }
        };
    }

    enum ReturnStrategy {
        LIST, SLICE, ONE_VALUE;
    }

    /**
     *
     */
    private class IgniteRepositoryQuery implements RepositoryQuery {
        /** Type. */
        private final Class<?> type;
        /** Sql. */
        private final IgniteQuery query;
        /** Cache. */
        private final IgniteCache cache;

        /** Method. */
        private final Method mtd;
        /** Metadata. */
        private final RepositoryMetadata metadata;
        /** Factory. */
        private final ProjectionFactory factory;

        /** Return strategy. */
        private final ReturnStrategy returnStrategy;

        public IgniteRepositoryQuery(RepositoryMetadata metadata, IgniteQuery query,
            Method mtd, ProjectionFactory factory) {
            type = metadata.getDomainType();
            this.query = query;
            cache = ignite.getOrCreateCache(cacheNameForRepos.get(metadata.getRepositoryInterface()));

            this.metadata = metadata;
            this.mtd = mtd;
            this.factory = factory;

            Class<?> returnType = mtd.getReturnType();

            if (returnType.isAssignableFrom(ArrayList.class))
                returnStrategy = ReturnStrategy.LIST;
            else if (returnType == Slice.class)
                returnStrategy = ReturnStrategy.SLICE;
            else
                returnStrategy = ReturnStrategy.ONE_VALUE;
        }

        /** {@inheritDoc} */
        @Override public Object execute(Object[] prmtrs) {
            Object[] parameters = prmtrs;
            String sql = this.query.sql();

            org.apache.ignite.cache.query.Query qry;

            switch (query.dynamicity()) {
                case SORTABLE:
                    sql = IgniteQueryGenerator.addSorting(new StringBuilder(sql),
                        (Sort) parameters[parameters.length - 1]).toString();
                    parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);
                    break;
                case PAGEBABLE:
                    sql = IgniteQueryGenerator.addPaging(new StringBuilder(sql),
                        (Pageable) parameters[parameters.length - 1]).toString();
                    parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);
                    break;
            }

            if (query.isFieldQuery()) {
                SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(sql);
                sqlFieldsQry.setArgs(parameters);
                qry = sqlFieldsQry;
            } else {
                SqlQuery sqlQry = new SqlQuery(type, sql);
                sqlQry.setArgs(parameters);
                qry = sqlQry;
            }

            QueryCursor qryCursor = cache.query(qry);

            Iterable<CacheEntryImpl> qryIter = (Iterable<CacheEntryImpl>)qryCursor;

            switch (returnStrategy) {
                case LIST:
                    ArrayList list = new ArrayList();

                    for (CacheEntryImpl entry : qryIter)
                        list.add(entry.getValue());

                    return list;
                case ONE_VALUE:
                    Iterator<CacheEntryImpl> iter = qryIter.iterator();
                    if (iter.hasNext())
                        return iter.next();

                    return null;
                case SLICE:
                    ArrayList content = new ArrayList();

                    for (CacheEntryImpl entry : qryIter)
                        content.add(entry.getValue());

                    return new SliceImpl(content, (Pageable)prmtrs[prmtrs.length - 1], true);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public QueryMethod getQueryMethod() {
            return new QueryMethod(mtd, metadata, factory);
        }
    }
}