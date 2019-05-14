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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorQueryTask}.
 */
public class VisorQueryTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name for query. */
    private String cacheName;

    /** Query text. */
    private String qryTxt;

    /** Distributed joins enabled flag. */
    private boolean distributedJoins;

    /** Enforce join order flag. */
    private boolean enforceJoinOrder;

    /** Query contains only replicated tables flag.*/
    private boolean replicatedOnly;

    /** Flag whether to execute query locally. */
    private boolean loc;

    /** Result batch size. */
    private int pageSize;

    /** Lazy query execution flag */
    private boolean lazy;

    /** Collocation flag. */
    private boolean collocated;

    /**
     * Default constructor.
     */
    public VisorQueryTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name for query.
     * @param qryTxt Query text.
     * @param distributedJoins If {@code true} then distributed joins enabled.
     * @param enforceJoinOrder If {@code true} then enforce join order.
     * @param replicatedOnly {@code true} then query contains only replicated tables.
     * @param loc Flag whether to execute query locally.
     * @param pageSize Result batch size.
     */
    public VisorQueryTaskArg(
        String cacheName,
        String qryTxt,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean replicatedOnly,
        boolean loc,
        int pageSize
    ) {
        this(cacheName, qryTxt, distributedJoins, enforceJoinOrder, replicatedOnly, loc, pageSize, false, false);
    }

    /**
     * @param cacheName Cache name for query.
     * @param qryTxt Query text.
     * @param distributedJoins If {@code true} then distributed joins enabled.
     * @param enforceJoinOrder If {@code true} then enforce join order.
     * @param replicatedOnly {@code true} then query contains only replicated tables.
     * @param loc Flag whether to execute query locally.
     * @param pageSize Result batch size.
     * @param lazy Lazy query execution flag.
     */
    public VisorQueryTaskArg(
        String cacheName,
        String qryTxt,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean replicatedOnly,
        boolean loc,
        int pageSize,
        boolean lazy
    ) {
        this(cacheName, qryTxt, distributedJoins, enforceJoinOrder, replicatedOnly, loc, pageSize, lazy, false);
    }

    /**
     * @param cacheName Cache name for query.
     * @param qryTxt Query text.
     * @param distributedJoins If {@code true} then distributed joins enabled.
     * @param enforceJoinOrder If {@code true} then enforce join order.
     * @param replicatedOnly {@code true} then query contains only replicated tables.
     * @param loc Flag whether to execute query locally.
     * @param pageSize Result batch size.
     * @param lazy Lazy query execution flag.
     * @param collocated Collocation flag.
     */
    public VisorQueryTaskArg(
        String cacheName,
        String qryTxt,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean replicatedOnly,
        boolean loc,
        int pageSize,
        boolean lazy,
        boolean collocated
    ) {
        this.cacheName = cacheName;
        this.qryTxt = qryTxt;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.replicatedOnly = replicatedOnly;
        this.loc = loc;
        this.pageSize = pageSize;
        this.lazy = lazy;
        this.collocated = collocated;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Query txt.
     */
    public String getQueryText() {
        return qryTxt;
    }

    /**
     * @return Distributed joins enabled flag.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return {@code true} If the query contains only replicated tables.
     */
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @return {@code true} If query should be executed locally.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Lazy query execution flag.
     *
     * @return Lazy flag.
     */
    public boolean getLazy() {
        return lazy;
    }

    /**
     * Flag indicating if this query is collocated.
     *
     * @return {@code true} If the query is collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeString(out, qryTxt);
        out.writeBoolean(distributedJoins);
        out.writeBoolean(enforceJoinOrder);
        out.writeBoolean(loc);
        out.writeInt(pageSize);
        out.writeBoolean(lazy);
        out.writeBoolean(collocated);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        qryTxt = U.readString(in);
        distributedJoins = in.readBoolean();
        enforceJoinOrder = in.readBoolean();
        loc = in.readBoolean();
        pageSize = in.readInt();

        if (protoVer > V1)
            lazy = in.readBoolean();

        if (protoVer > V2)
            collocated = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryTaskArg.class, this);
    }
}
