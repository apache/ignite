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
 *
 */

package org.apache.ignite.internal.processors.query.running;

import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/** {@link TrackableQuery} implementation that facilitates processing of query information in {@link HeavyQueriesTracker}. */
public class TrackableQueryImpl implements TrackableQuery {
    /** Schema name. */
    private String schema;

    /** Node id. */
    private UUID nodeId;

    /** Query id. */
    private long qryId;

    /** @return Schema name. */
    public String schema() {
        return schema;
    }

    /**
     * @param schema Schema name.
     *
     * @return {@code this} for chaining.
     */
    public TrackableQueryImpl schema(String schema) {
        this.schema = schema;

        return this;
    }

    /** @return Node id. */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node id.
     *
     * @return {@code this} for chaining.
     */
    public TrackableQueryImpl nodeId(UUID nodeId) {
        this.nodeId = nodeId;

        return this;
    }

    /** @return Query id. */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query id.
     *
     * @return {@code this} for chaining.
     */
    public TrackableQueryImpl queryId(long qryId) {
        this.qryId = qryId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof TrackableQueryImpl))
            return false;

        TrackableQueryImpl info = (TrackableQueryImpl)o;

        return schema().equals(info.schema()) &&
            nodeId().equals(info.nodeId()) &&
            queryId() == info.queryId();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema(), nodeId(), queryId());
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String queryInfo(@Nullable String additinalInfo) {
        return null;
    }
}
