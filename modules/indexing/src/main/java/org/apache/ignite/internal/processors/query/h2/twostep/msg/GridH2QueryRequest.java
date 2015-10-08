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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Query request.
 */
public class GridH2QueryRequest implements Message, GridH2MarshallableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Map query will not destroy context until explicit query cancel request will be received. */
    public static int FLAG_EXPLICIT_QRY_FINISH = 1;

    /** */
    private long reqId;

    /** */
    @GridToStringInclude
    @GridDirectCollection(String.class)
    private List<String> caches;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Explicit partitions mappings for nodes. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = int[].class)
    private Map<UUID, int[]> parts;

    /** */
    private int pageSize;

    /** */
    @GridToStringInclude
    @GridDirectCollection(Message.class)
    private List<GridCacheSqlQuery> qrys;

    /** */
    private byte flags;

    /**
     * @param reqId Request ID.
     * @return {@code this}.
     */
    public GridH2QueryRequest requestId(long reqId) {
        this.reqId = reqId;

        return this;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param caches Caches.
     * @return {@code this}.
     */
    public GridH2QueryRequest caches(List<String> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * @return Caches.
     */
    public List<String> caches() {
        return caches;
    }

    /**
     * @param topVer Topology version.
     * @return {@code this}.
     */
    public GridH2QueryRequest topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;

        return this;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Explicit partitions mapping.
     */
    public Map<UUID,int[]> partitions() {
        return parts;
    }

    /**
     * @param parts Explicit partitions mapping.
     * @return {@code this}.
     */
    public GridH2QueryRequest partitions(Map<UUID,int[]> parts) {
        this.parts = parts;

        return this;
    }

    /**
     * @param pageSize Page size.
     * @return {@code this}.
     */
    public GridH2QueryRequest pageSize(int pageSize) {
        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param qrys SQL Queries.
     * @return {@code this}.
     */
    public GridH2QueryRequest queries(List<GridCacheSqlQuery> qrys) {
        this.qrys = qrys;

        return this;
    }

    /**
     * @return SQL Queries.
     */
    public List<GridCacheSqlQuery> queries() {
        return qrys;
    }

    /**
     * @param flags Flags.
     * @return {@code this}.
     */
    public GridH2QueryRequest flags(int flags) {
        this.flags = (byte)flags;

        return this;
    }

    /**
     * @param flags Flags to check.
     * @return {@code true} If all the requested flags are set to {@code true}.
     */
    public boolean isFlagSet(int flags) {
        return (this.flags & flags) == flags;
    }

    /** {@inheritDoc} */
    @Override public void marshall(Marshaller m) {
        if (qrys != null) {
            for (GridCacheSqlQuery qry : qrys)
                qry.marshallParams(m);
        }
    }

    /** {@inheritDoc} */
    @Override public void unmarshall(Marshaller m) {
        if (qrys != null) {
            for (GridCacheSqlQuery qry : qrys)
                qry.unmarshallParams(m);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2QueryRequest.class, this);
    }
}
