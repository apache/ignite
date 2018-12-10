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

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Query.
 */
public class GridCacheSqlQuery implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final Object[] EMPTY_PARAMS = {};

    /** */
    @GridToStringInclude(sensitive = true)
    private String qry;

    /** */
    @GridToStringInclude
    private int[] paramIdxs;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private LinkedHashMap<String, ?> cols;

    /** Field kept for backward compatibility. */
    private String alias;

    /** Sort columns. */
    @GridToStringInclude
    @GridDirectTransient
    private transient List<?> sort;

    /** If we have partitioned tables in this query. */
    @GridToStringInclude
    @GridDirectTransient
    private transient boolean partitioned;

    /** Single node to execute the query on. */
    private UUID node;

    /** Derived partition info. */
    @GridToStringInclude
    @GridDirectTransient
    private transient Object derivedPartitions;

    /** Flag indicating that query contains sub-queries. */
    @GridToStringInclude
    @GridDirectTransient
    private transient boolean hasSubQries;

    /**
     * For {@link Message}.
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
     * @return {@code this}.
     */
    public GridCacheSqlQuery query(String qry) {
        this.qry = qry;

        return this;
    }

    /**
     * @return Parameter indexes.
     */
    public int[] parameterIndexes() {
        return paramIdxs;
    }

    /**
     * @param paramIdxs Parameter indexes.
     * @return {@code this}.
     */
    public GridCacheSqlQuery parameterIndexes(int[] paramIdxs) {
        this.paramIdxs = paramIdxs;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSqlQuery.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString("alias", alias))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("node", node))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIntArray("paramIdxs", paramIdxs))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeString("qry", qry))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                alias = reader.readString("alias");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                node = reader.readUuid("node");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                paramIdxs = reader.readIntArray("paramIdxs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qry = reader.readString("qry");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheSqlQuery.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 112;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
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
     * @return {@code this}.
     */
    public GridCacheSqlQuery node(UUID node) {
        this.node = node;

        return this;
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
     * @param hasSubQries Flag indicating that query contains sub-queries.
     *
     * @return {@code this}.
     */
    public GridCacheSqlQuery hasSubQueries(boolean hasSubQries) {
        this.hasSubQries = hasSubQries;

        return this;
    }
}
