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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLSerializer;
import com.querydsl.sql.SQLTemplates;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.springdata22.repository.query.IgniteQuery;
import org.apache.ignite.springdata22.repository.query.IgniteQueryGenerator;
import org.apache.ignite.springdata22.repository.query.QueryUtils;
import org.apache.ignite.springdata22.repository.query.ReturnStrategy;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.RepositoryMetadata;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.springdata22.repository.query.QueryUtils.transformQueryCursor;

/**
 * General Apache Ignite query predicate executor implementation.
 * This bean shouldn't ever be loaded by context directly, only via {@link IgniteRepositoryFactory}
 */
public class QueryPredicateExecutorImpl<T> implements QuerydslPredicateExecutor<T> {
    /** Config for SQL serializer. */
    private final Configuration conf = new Configuration(SQLTemplates.DEFAULT);

    /** Underlying cache to execute SQL queries. */
    private final IgniteCache cache;

    /** Result type. */
    private final Class<?> type;

    /**
     * @param cache Cache.
     * @param metadata Metadata.
     */
    public QueryPredicateExecutorImpl(IgniteCache cache, RepositoryMetadata metadata) {
        this.cache = cache;
        this.type = metadata.getDomainType();
    }

    /** {@inheritDoc} */
    @Override public Optional<T> findOne(Predicate pred) {
        return executeQuery(prepareSelectQuery(pred), PageRequest.of(0, 1)).stream().findFirst();
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll(Predicate pred) {
        return executeQuery(prepareSelectQuery(pred));
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll(Predicate pred, Sort sort) {
        return executeQuery(prepareSelectQuery(pred), sort);
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll(Predicate pred, OrderSpecifier<?>... orders) {
        Sort sort = Sort.by(mapQryDslOrderSpecifiersToOrders(orders));

        return executeQuery(prepareSelectQuery(pred), sort);
    }

    /** {@inheritDoc} */
    @Override public Iterable<T> findAll(OrderSpecifier<?>... orders) {
        Sort sort = Sort.by(mapQryDslOrderSpecifiersToOrders(orders));

        return executeQuery(prepareSqlQuery("SELECT * FROM " + type.getSimpleName()), sort);
    }

    /** {@inheritDoc} */
    @Override public Page<T> findAll(Predicate pred, Pageable pageable) {
        return new PageImpl<>(executeQuery(prepareSelectQuery(pred), pageable), pageable, 0);
    }

    /** {@inheritDoc} */
    @Override public long count(Predicate pred) {
        SQLSerializer ser = prepareSqlQuery("SELECT COUNT(*) FROM " + type.getSimpleName() + " WHERE ", pred);

        SqlFieldsQuery qry = new SqlFieldsQuery(ser.toString());
        qry.setArgs(ser.getConstants().toArray());

        List<List<?>> res = cache.query(qry).getAll();

        return (Long) res.get(0).get(0);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(Predicate pred) {
        return !executeQuery(prepareSelectQuery(pred), PageRequest.of(0, 1)).isEmpty();
    }

    /**
     * @param pred Predicate.
     * @return Prepared sql serializer.
     */
    private SQLSerializer prepareSelectQuery(Predicate pred) {
        return prepareSqlQuery(
            "SELECT * FROM " + type.getSimpleName() + " WHERE ",
            pred
        );
    }

    /**
     * @param selectPart Select part.
     * @return Prepared sql serializer.
     */
    private SQLSerializer prepareSqlQuery(String selectPart) {
        return prepareSqlQuery(selectPart, null);
    }

    /**
     * @param selectPart Select partition.
     * @param pred Predicate.
     * @return Prepared sql serializer.
     */
    private SQLSerializer prepareSqlQuery(String selectPart, Predicate pred) {
        SQLSerializer res = new SQLSerializer(conf).append(selectPart + " ");

        if (pred != null)
            return res.handle(pred);

        return res;
    }

    /**
     * @param ser Serializable.
     * @param additionalParams Additional params.
     * @return List of results.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<T> executeQuery(SQLSerializer ser, Object... additionalParams) {
        List<Object> params = ser.getConstants();
        params.addAll(Arrays.asList(additionalParams));

        Object[] arrParams = params.toArray();

        IgniteQuery qry = IgniteQueryGenerator.prepareQuery(type, ser.toString(), arrParams);

        try (QueryCursor qryCursor = cache.query(QueryUtils.prepareQuery(qry, arrParams))) {
            return (List<T>) transformQueryCursor(qryCursor, arrParams, false, ReturnStrategy.LIST_OF_VALUES);
        }
    }

    /**
     * @param orders Orders.
     */
    private List<Sort.Order> mapQryDslOrderSpecifiersToOrders(OrderSpecifier<?>... orders) {
        return Arrays.stream(orders).map(this::mapQryDslOrderSpecifierToOrder).collect(toList());
    }

    /**
     * @param order Order.
     * @return Spring data sort order.
     */
    private Sort.Order mapQryDslOrderSpecifierToOrder(OrderSpecifier<?> order) {
        Sort.Direction direction = Sort.Direction.valueOf(order.getOrder().name());

        Sort.NullHandling handling;

        switch (order.getNullHandling()) {
            case NullsFirst:
                handling = Sort.NullHandling.NULLS_FIRST;
                break;

            case NullsLast:
                handling = Sort.NullHandling.NULLS_LAST;
                break;

            case Default:
            default:
                handling = Sort.NullHandling.NATIVE;
                break;
        }

        return new Sort.Order(direction, order.getTarget().toString(), handling);
    }
}
