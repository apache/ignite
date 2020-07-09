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

package org.apache.ignite.springdata.repository.query;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

import static org.apache.ignite.springdata.repository.query.IgniteQueryGenerator.addPaging;
import static org.apache.ignite.springdata.repository.query.IgniteQueryGenerator.addSorting;

/**
 * Ignite SQL query implementation.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class IgniteRepositoryQuery implements RepositoryQuery {
    /** Defines the way how to process query result */
    private enum ReturnStrategy {
        /** Need to return only one value. */
        ONE_VALUE,

        /** Need to return one cache entry */
        CACHE_ENTRY,

        /** Need to return list of cache entries */
        LIST_OF_CACHE_ENTRIES,

        /** Need to return list of values */
        LIST_OF_VALUES,

        /** Need to return list of lists */
        LIST_OF_LISTS,

        /** Need to return slice */
        SLICE_OF_VALUES,

        /** Slice of cache entries. */
        SLICE_OF_CACHE_ENTRIES,

        /** Slice of lists. */
        SLICE_OF_LISTS
    }

    /** Type. */
    private final Class<?> type;

    /** Sql. */
    private final IgniteQuery qry;

    /** Cache. */
    private final IgniteCache cache;

    /** Method. */
    private final Method mtd;

    /** Metadata. */
    private final RepositoryMetadata metadata;

    /** Factory. */
    private final ProjectionFactory factory;

    /** Return strategy. */
    private final ReturnStrategy returnStgy;

    /**
     * @param metadata Metadata.
     * @param qry Query.
     * @param mtd Method.
     * @param factory Factory.
     * @param cache Cache.
     */
    public IgniteRepositoryQuery(RepositoryMetadata metadata, IgniteQuery qry,
        Method mtd, ProjectionFactory factory, IgniteCache cache) {
        type = metadata.getDomainType();
        this.qry = qry;
        this.cache = cache;
        this.metadata = metadata;
        this.mtd = mtd;
        this.factory = factory;

        returnStgy = calcReturnType(mtd, qry.isFieldQuery());
    }

    /** {@inheritDoc} */
    @Override public Object execute(Object[] prmtrs) {
        Query qry = prepareQuery(prmtrs);

        try (QueryCursor qryCursor = cache.query(qry)) {
            return transformQueryCursor(prmtrs, qryCursor);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMethod getQueryMethod() {
        return new QueryMethod(mtd, metadata, factory);
    }

    /**
     * @param mtd Method.
     * @param isFieldQry Is field query.
     * @return Return strategy type.
     */
    private ReturnStrategy calcReturnType(Method mtd, boolean isFieldQry) {
        Class<?> returnType = mtd.getReturnType();

        if (returnType.isAssignableFrom(ArrayList.class)) {
            if (isFieldQry) {
                if (hasAssignableGenericReturnTypeFrom(ArrayList.class, mtd))
                    return ReturnStrategy.LIST_OF_LISTS;
            }
            else if (hasAssignableGenericReturnTypeFrom(Cache.Entry.class, mtd))
                return ReturnStrategy.LIST_OF_CACHE_ENTRIES;

            return ReturnStrategy.LIST_OF_VALUES;
        }
        else if (returnType == Slice.class) {
            if (isFieldQry) {
                if (hasAssignableGenericReturnTypeFrom(ArrayList.class, mtd))
                    return ReturnStrategy.SLICE_OF_LISTS;
            }
            else if (hasAssignableGenericReturnTypeFrom(Cache.Entry.class, mtd))
                return ReturnStrategy.SLICE_OF_CACHE_ENTRIES;

            return ReturnStrategy.SLICE_OF_VALUES;
        }
        else if (Cache.Entry.class.isAssignableFrom(returnType))
            return ReturnStrategy.CACHE_ENTRY;
        else
            return ReturnStrategy.ONE_VALUE;
    }

    /**
     * @param cls Class 1.
     * @param mtd Method.
     * @return if {@code mtd} return type is assignable from {@code cls}
     */
    private boolean hasAssignableGenericReturnTypeFrom(Class<?> cls, Method mtd) {
        Type[] actualTypeArguments = ((ParameterizedType)mtd.getGenericReturnType()).getActualTypeArguments();

        if (actualTypeArguments.length == 0)
            return false;

        if (actualTypeArguments[0] instanceof ParameterizedType) {
            ParameterizedType type = (ParameterizedType)actualTypeArguments[0];

            Class<?> type1 = (Class)type.getRawType();

            return type1.isAssignableFrom(cls);
        }

        if (actualTypeArguments[0] instanceof Class) {
            Class typeArg = (Class)actualTypeArguments[0];

            return typeArg.isAssignableFrom(cls);
        }

        return false;
    }

    /**
     * @param prmtrs Prmtrs.
     * @param qryCursor Query cursor.
     * @return Query cursor or slice
     */
    @Nullable private Object transformQueryCursor(Object[] prmtrs, QueryCursor qryCursor) {
        if (qry.isFieldQuery()) {
            Iterable<List> qryIter = (Iterable<List>)qryCursor;

            switch (returnStgy) {
                case LIST_OF_VALUES:
                    List list = new ArrayList<>();

                    for (List entry : qryIter)
                        list.add(entry.get(0));

                    return list;

                case ONE_VALUE:
                    Iterator<List> iter = qryIter.iterator();

                    if (iter.hasNext())
                        return iter.next().get(0);

                    return null;

                case SLICE_OF_VALUES:
                    List content = new ArrayList<>();

                    for (List entry : qryIter)
                        content.add(entry.get(0));

                    return new SliceImpl(content, (Pageable)prmtrs[prmtrs.length - 1], true);

                case SLICE_OF_LISTS:
                    return new SliceImpl(qryCursor.getAll(), (Pageable)prmtrs[prmtrs.length - 1], true);

                case LIST_OF_LISTS:
                    return qryCursor.getAll();

                default:
                    throw new IllegalStateException();
            }
        }
        else {
            Iterable<CacheEntryImpl> qryIter = (Iterable<CacheEntryImpl>)qryCursor;

            switch (returnStgy) {
                case LIST_OF_VALUES:
                    List list = new ArrayList<>();

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

                case SLICE_OF_VALUES:
                    List content = new ArrayList<>();

                    for (CacheEntryImpl entry : qryIter)
                        content.add(entry.getValue());

                    return new SliceImpl(content, (Pageable)prmtrs[prmtrs.length - 1], true);

                case SLICE_OF_CACHE_ENTRIES:
                    return new SliceImpl(qryCursor.getAll(), (Pageable)prmtrs[prmtrs.length - 1], true);

                case LIST_OF_CACHE_ENTRIES:
                    return qryCursor.getAll();

                default:
                    throw new IllegalStateException();
            }
        }
    }

    /**
     * @param prmtrs Prmtrs.
     * @return prepared query for execution
     */
    @SuppressWarnings("deprecation")
    @NotNull private Query prepareQuery(Object[] prmtrs) {
        Object[] parameters = prmtrs;
        String sql = qry.sql();

        switch (qry.options()) {
            case SORTING:
                sql = addSorting(new StringBuilder(sql), (Sort)parameters[parameters.length - 1]).toString();
                parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);

                break;

            case PAGINATION:
                sql = addPaging(new StringBuilder(sql), (Pageable)parameters[parameters.length - 1]).toString();
                parameters = Arrays.copyOfRange(parameters, 0, parameters.length - 1);

                break;

            case NONE:
                // No-op.
        }

        if (qry.isFieldQuery()) {
            SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(sql);
            sqlFieldsQry.setArgs(parameters);

            return sqlFieldsQry;
        }

        SqlQuery sqlQry = new SqlQuery(type, sql);
        sqlQry.setArgs(parameters);

        return sqlQry;
    }
}
