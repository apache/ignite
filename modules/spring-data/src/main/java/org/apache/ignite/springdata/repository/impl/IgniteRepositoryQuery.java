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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 *
 */
class IgniteRepositoryQuery implements RepositoryQuery {
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
    private final IgniteRepositoryFactory.ReturnStrategy returnStrategy;

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
                returnStrategy = IgniteRepositoryFactory.ReturnStrategy.LIST;
            else {
                if (actualTypeArguments[0] instanceof ParameterizedType) {
                    ParameterizedType type = (ParameterizedType)actualTypeArguments[0];
                    Class type1 = (Class)type.getRawType();

                    if (Cache.Entry.class.isAssignableFrom(type1))
                        returnStrategy = IgniteRepositoryFactory.ReturnStrategy.LIST_OF_CACHE_ENTRIES;
                    else
                        returnStrategy = IgniteRepositoryFactory.ReturnStrategy.LIST;
                } else
                    returnStrategy = IgniteRepositoryFactory.ReturnStrategy.LIST;
            }
        } else if (returnType == Slice.class)
            returnStrategy = IgniteRepositoryFactory.ReturnStrategy.SLICE;
        else if (Cache.Entry.class.isAssignableFrom(returnType))
            returnStrategy = IgniteRepositoryFactory.ReturnStrategy.CACHE_ENTRY;
        else
            returnStrategy = IgniteRepositoryFactory.ReturnStrategy.ONE_VALUE;
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
