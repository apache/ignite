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

/**
 * {@link TrackableQuery} implementation that facilitates identifying H2 queries information in {@link HeavyQueriesTracker}
 * from the {@code ignite-core} module.
 */
public class TrackableQueryImpl implements TrackableQuery {
    /** Query schema. */
    private String schema;

    /** Query SQL. */
    private String sql;

    /** Originator node uid. */
    private UUID nodeId;

    /** Query id. */
    private long qryId;

    /** */
    public String schema() {
        return schema;
    }

    /** */
    public TrackableQueryImpl schema(String schema) {
        this.schema = schema;

        return this;
    }

    /** */
    public String sql() {
        return sql;
    }

    /** */
    public TrackableQueryImpl sql(String sql) {
        this.sql = sql;

        return this;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public TrackableQueryImpl nodeId(UUID nodeId) {
        this.nodeId = nodeId;

        return this;
    }

    /** */
    public long queryId() {
        return qryId;
    }

    /** */
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
            sql().equals(info.sql()) &&
            nodeId().equals(info.nodeId()) &&
            queryId() == info.queryId();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema(), sql(), nodeId(), queryId());
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
