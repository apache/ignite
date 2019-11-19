/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action.query;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for query argument.
 */
public class QueryArgument {
    /** Query ID. */
    private String qryId;

    /** Cache name for query. */
    private String cacheName;

    /** Query text. */
    private String qryTxt;

    /** Parameters. */
    private Object[] parameters;

    /** Distributed joins enabled flag. */
    private boolean distributedJoins;

    /** Enforce join order flag. */
    private boolean enforceJoinOrder;

    /** Target node ID. */
    private UUID targetNodeId;

    /** Result batch size. */
    private int pageSize;

    /** Lazy query execution flag */
    private boolean lazy;

    /** Collocation flag. */
    private boolean collocated;

    /**
     * @return Query ID.
     */
    public String getQueryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setQueryId(String qryId) {
        this.qryId = qryId;

        return this;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setCacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    /**
     * @return Query text.
     */
    public String getQueryText() {
        return qryTxt;
    }

    /**
     * @param qryTxt Query txt.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setQueryText(String qryTxt) {
        this.qryTxt = qryTxt;

        return this;
    }

    /**
     * @return Query parameters.
     */
    public Object[] getParameters() {
        return parameters;
    }

    /**
     * @param parameters Parameters.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setParameters(Object[] parameters) {
        this.parameters = parameters;

        return this;
    }

    /**
     * @return @{code true} if distributed joins should be used.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * @param distributedJoins Distributed joins.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;

        return this;
    }

    /**
     * @return @{code true} if enforce join order should be used.
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @param enforceJoinOrder Enforce join order.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setEnforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;

        return this;
    }

    /**
     * @return Node ID on which query will be executed.
     */
    public UUID getTargetNodeId() {
        return targetNodeId;
    }

    /**
     * @param targetNodeId Target node id.
     * @return {@code This} for chaining method calls.
     */
    public QueryArgument setTargetNodeId(UUID targetNodeId) {
        this.targetNodeId = targetNodeId;

        return this;
    }

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     * @return This for chaining method calls.
     */
    public QueryArgument setPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return @{code true} if it's lazy query.
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * @param lazy Lazy.
     * @return This for chaining method calls.
     */
    public QueryArgument setLazy(boolean lazy) {
        this.lazy = lazy;

        return this;
    }

    /**
     * @return @{code true} if it's collocated query.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param collocated Collocated.
     * @return This for chaining method calls.
     */
    public QueryArgument setCollocated(boolean collocated) {
        this.collocated = collocated;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryArgument.class, this);
    }
}
