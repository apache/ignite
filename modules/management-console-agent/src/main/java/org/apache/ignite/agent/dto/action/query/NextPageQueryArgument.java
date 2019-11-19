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
 * DTO for next page query argument.
 */
public class NextPageQueryArgument {
    /** Query ID. */
    private  String qryId;

    /** Cursor ID. */
    private String cursorId;

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
     * @param qryId Query id.
     * @return {@code This} for chaining method calls.
     */
    public NextPageQueryArgument setQueryId(String qryId) {
        this.qryId = qryId;

        return this;
    }

    /**
     * @return Cursor ID.
     */
    public String getCursorId() {
        return cursorId;
    }

    /**
     * @param cursorId Cursor id.
     * @return {@code This} for chaining method calls.
     */
    public NextPageQueryArgument setCursorId(String cursorId) {
        this.cursorId = cursorId;

        return this;
    }

    /**
     * @return Request page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     * @return {@code This} for chaining method calls.
     */
    public NextPageQueryArgument setPageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Node ID on which next page query will be executed.
     */
    public UUID getTargetNodeId() {
        return targetNodeId;
    }

    /**
     * @param targetNodeId Target node id.
     * @return {@code This} for chaining method calls.
     */
    public NextPageQueryArgument setTargetNodeId(UUID targetNodeId) {
        this.targetNodeId = targetNodeId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NextPageQueryArgument.class, this);
    }
}
