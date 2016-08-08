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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
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
                            new IgniteQuery(qryStr, isFieldQuery(qryStr), IgniteQueryGenerator.isDynamic(mtd)), mtd, factory,
                                ignite.getOrCreateCache(cacheNameForRepos.get(metadata.getRepositoryInterface())));
                }
                //TODO namedQueries handling

                if (key == Key.USE_DECLARED_QUERY)
                    throw new IllegalStateException();

                return new IgniteRepositoryQuery(
                    metadata,
                    IgniteQueryGenerator.generateSql(mtd, metadata),
                    mtd,
                    factory,
                    ignite.getOrCreateCache(cacheNameForRepos.get(metadata.getRepositoryInterface())));
            }
        };
    }

    private boolean isFieldQuery(String s) {
        return s.matches("^SELECT.*") && !s.matches("^SELECT\\s+(?:\\w+\\.)?+\\*.*");
    }

    enum ReturnStrategy {
        ONE_VALUE, CACHE_ENTRY, LIST_OF_CACHE_ENTRIES, LIST, LIST_OF_LISTS, SLICE;
    }

    /**
     *
     */
    private static class IgniteRepositoryQuery implements RepositoryQuery {
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
            Method mtd, ProjectionFactory factory, IgniteCache cache) {
            type = metadata.getDomainType();
            this.query = query;
            this.cache = cache;

            this.metadata = metadata;
            this.mtd = mtd;
            this.factory = factory;

            Class<?> returnType = mtd.getReturnType();

            if (returnType.isAssignableFrom(ArrayList.class)) {
                Type[] actualTypeArguments = ((ParameterizedType)mtd.getGenericReturnType()).getActualTypeArguments();

                if (actualTypeArguments.length == 0 )
                    returnStrategy = ReturnStrategy.LIST;
                else {
                    if (actualTypeArguments[0] instanceof ParameterizedType) {
                        ParameterizedType type = (ParameterizedType)actualTypeArguments[0];
                        Class type1 = (Class)type.getRawType();

                        if (Cache.Entry.class.isAssignableFrom(type1))
                            returnStrategy = ReturnStrategy.LIST_OF_CACHE_ENTRIES;
                        else
                            returnStrategy = ReturnStrategy.LIST;
                    } else
                        returnStrategy = ReturnStrategy.LIST;
                }
            } else if (returnType == Slice.class)
                returnStrategy = ReturnStrategy.SLICE;
            else if (Cache.Entry.class.isAssignableFrom(returnType))
                returnStrategy = ReturnStrategy.CACHE_ENTRY;
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
                        (Sort)parameters[parameters.length - 1]).toString();
                    parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);
                    break;
                case PAGEBABLE:
                    sql = IgniteQueryGenerator.addPaging(new StringBuilder(sql),
                        (Pageable)parameters[parameters.length - 1]).toString();
                    parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);
                    break;
            }

            if (query.isFieldQuery()) {
                SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(sql);
                sqlFieldsQry.setArgs(parameters);
                qry = sqlFieldsQry;
            }
            else {
                SqlQuery sqlQry = new SqlQuery(type, sql);
                sqlQry.setArgs(parameters);
                qry = sqlQry;
            }

            QueryCursor qryCursor = cache.query(qry);

            if (query.isFieldQuery()) {
                Iterable<ArrayList> qryIter = (Iterable<ArrayList>)qryCursor;

                switch (returnStrategy) {
                    case LIST:
                        ArrayList list = new ArrayList();

                        for (ArrayList entry : qryIter)
                            list.add(entry.get(0));

                        return list;
                    case ONE_VALUE:
                        Iterator<ArrayList> iter = qryIter.iterator();
                        if (iter.hasNext())
                            return iter.next().get(0);

                        return null;
                    case SLICE:
                        ArrayList content = new ArrayList();

                        for (ArrayList entry : qryIter)
                            content.add(entry.get(0));

                        return new SliceImpl(content, (Pageable)prmtrs[prmtrs.length - 1], true);
                    default:
                        throw new IllegalStateException();
                }
            }
            else {
                Iterable<CacheEntryImpl> qryIter = (Iterable<CacheEntryImpl>)qryCursor;

                switch (returnStrategy) {
                    case LIST:
                        ArrayList list = new ArrayList();

                        for (CacheEntryImpl entry : qryIter)
                            list.add(entry.getValue());

                        return list;
                    case ONE_VALUE:
                        Iterator<CacheEntryImpl> iter1 = qryIter.iterator();
                        if (iter1.hasNext())
                            return iter1.next().getValue();

                        return null;
                    case CACHE_ENTRY:
                        Iterator<CacheEntryImpl> iter2 = qryIter.iterator();
                        if (iter2.hasNext())
                            return iter2.next();

                        return null;
                    case SLICE:
                        ArrayList content = new ArrayList();

                        for (CacheEntryImpl entry : qryIter)
                            content.add(entry.getValue());

                        return new SliceImpl(content, (Pageable)prmtrs[prmtrs.length - 1], true);
                    case LIST_OF_CACHE_ENTRIES:
                        return qryCursor.getAll();
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        /** {@inheritDoc} */
        @Override public QueryMethod getQueryMethod() {
            return new QueryMethod(mtd, metadata, factory);
        }
    }
}