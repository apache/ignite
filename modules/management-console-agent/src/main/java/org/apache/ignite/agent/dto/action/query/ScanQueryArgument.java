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
 * DTO fro scan query argument.
 */
// TODO GG-24423: Add SqlPredicate for filtering entries.
public class ScanQueryArgument {
    /** Query ID. */
    private String qryId;

    /** Cache name for query. */
    private String cacheName;

    /** Result batch size. */
    private int pageSize;

    /** Target node ID. */
    private UUID targetNodeId;

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
    public ScanQueryArgument setQueryId(String qryId) {
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
    public ScanQueryArgument setCacheName(String cacheName) {
        this.cacheName = cacheName;

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
     * @return {@code This} for chaining method calls.
     */
    public ScanQueryArgument setPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Target node ID.
     */
    public UUID getTargetNodeId() {
        return targetNodeId;
    }

    /**
     * @param targetNodeId Target node id.
     * @return {@code This} for chaining method calls.
     */
    public ScanQueryArgument setTargetNodeId(UUID targetNodeId) {
        this.targetNodeId = targetNodeId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ScanQueryArgument.class, this);
    }
}
