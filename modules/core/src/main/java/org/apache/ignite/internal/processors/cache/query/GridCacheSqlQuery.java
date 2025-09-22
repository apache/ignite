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

package org.apache.ignite.internal.processors.cache.query;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Query.
 */
public class GridCacheSqlQuery implements Message {
    /** */
    public static final Object[] EMPTY_PARAMS = {};

    /** */
    @Order(value = 0, method = "query")
    @GridToStringInclude(sensitive = true)
    private String qry;

    /** */
    @Order(value = 1, method = "parameterIndexes")
    @GridToStringInclude
    private int[] paramIdxs;

    /** */
    @GridToStringInclude
    private LinkedHashMap<String, ?> cols;

    /** Sort columns. */
    @GridToStringInclude
    private List<?> sort;

    /** If we have partitioned tables in this query. */
    @GridToStringInclude
    private boolean partitioned;

    /** Single node to execute the query on. */
    @Order(2)
    private UUID node;

    /** Derived partition info. */
    @GridToStringInclude
    private Object derivedPartitions;

    /** Flag indicating that query contains sub-queries. */
    @GridToStringInclude
    private boolean hasSubQries;

    /** Flag indicating that the query contains an OUTER JOIN from REPLICATED to PARTITIONED. */
    @GridToStringInclude
    private boolean treatPartitionedAsReplicated;

    /**
     * Empty constructor.
     */
    public GridCacheSqlQuery() {
        // No-op.
    }

    /**
     * @param qry Query.
     */
    public GridCacheSqlQuery(String qry) {
        A.ensure(!F.isEmpty(qry), "qry must not be empty");

        this.qry = qry;
    }

    /**
     * @return Columns.
     */
    public LinkedHashMap<String, ?> columns() {
        return cols;
    }

    /**
     * @param columns Columns.
     * @return {@code this}.
     */
    public GridCacheSqlQuery columns(LinkedHashMap<String, ?> columns) {
        this.cols = columns;

        return this;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @param qry Query.
     */
    public void query(String qry) {
        this.qry = qry;
    }

    /**
     * @return Parameter indexes.
     */
    public int[] parameterIndexes() {
        return paramIdxs;
    }

    /**
     * @param paramIdxs Parameter indexes.
     */
    public void parameterIndexes(int[] paramIdxs) {
        this.paramIdxs = paramIdxs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSqlQuery.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 112;
    }

    /**
     * @return Copy.
     */
    public GridCacheSqlQuery copy() {
        GridCacheSqlQuery cp = new GridCacheSqlQuery();

        cp.qry = qry;
        cp.cols = cols;
        cp.paramIdxs = paramIdxs;
        cp.sort = sort;
        cp.partitioned = partitioned;
        cp.derivedPartitions = derivedPartitions;
        cp.hasSubQries = hasSubQries;

        return cp;
    }

    /**
     * @param sort Sort columns.
     */
    public void sortColumns(List<?> sort) {
        this.sort = sort;
    }

    /**
     * @return Sort columns.
     */
    public List<?> sortColumns() {
        return sort;
    }

    /**
     * @param partitioned If the query contains partitioned tables.
     */
    public void partitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    /**
     * @return {@code true} If the query contains partitioned tables.
     */
    public boolean isPartitioned() {
        return partitioned;
    }

    /**
     * @return Single node to execute the query on or {@code null} if need to execute on all the nodes.
     */
    public UUID node() {
        return node;
    }

    /**
     * @param node Single node to execute the query on or {@code null} if need to execute on all the nodes.
     */
    public void node(UUID node) {
        this.node = node;
    }

    /**
     * @param allParams All parameters.
     * @return Parameters only for this query.
     */
    public Object[] parameters(Object[] allParams) {
        if (F.isEmpty(paramIdxs))
            return EMPTY_PARAMS;

        assert !F.isEmpty(allParams);

        int maxIdx = paramIdxs[paramIdxs.length - 1];

        Object[] res = new Object[maxIdx + 1];

        for (int i = 0; i < paramIdxs.length; i++) {
            int idx = paramIdxs[i];

            res[idx] = allParams[idx];
        }

        return res;
    }

    /**
     * @return Derived partitions.
     */
    public Object derivedPartitions() {
        return derivedPartitions;
    }

    /**
     * @param derivedPartitions Derived partitions.
     */
    public void derivedPartitions(Object derivedPartitions) {
        this.derivedPartitions = derivedPartitions;
    }

    /**
     * @return {@code true} if query contains sub-queries.
     */
    public boolean hasSubQueries() {
        return hasSubQries;
    }

    /**
     * @param hasSubQries Flag indicating that the query contains sub-queries.
     *
     * @return {@code this}.
     */
    public GridCacheSqlQuery hasSubQueries(boolean hasSubQries) {
        this.hasSubQries = hasSubQries;

        return this;
    }

    /**
     * @return {@code true} if the query contains an OUTER JOIN from REPLICATED to PARTITIONED, or
     * outer query over REPLICATED cache has a subquery over PARTIITIONED.
     */
    public boolean treatReplicatedAsPartitioned() {
        return treatPartitionedAsReplicated;
    }

    /**
     * Set flag to {@code true} when query contains an OUTER JOIN from REPLICATED to PARTITIONED, or
     * outer query over REPLICATED cache has a subquery over PARTIITIONED.
     *
     * @param trearPartitionedAsReplicated Flag indicating that the replicated cache in outer query must be treat
     * as partitioned.
     * @return {@code this}.
     */
    public GridCacheSqlQuery treatReplicatedAsPartitioned(boolean trearPartitionedAsReplicated) {
        this.treatPartitionedAsReplicated = trearPartitionedAsReplicated;

        return this;
    }
}
