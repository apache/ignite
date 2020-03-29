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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Descriptor of running query.
 */
public class VisorRunningQuery extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long id;

    /** Query text. */
    private String qry;

    /** Query type. */
    private GridCacheQueryType qryType;

    /** Schema name. */
    private String schemaName;

    /** */
    private long startTime;

    /** */
    private long duration;

    /** */
    private boolean cancellable;

    /** */
    private boolean loc;

    /**
     * Default constructor.
     */
    public VisorRunningQuery() {
        // No-op.
    }

    /**
     * Construct data transfer object for running query information.
     *
     * @param id Query ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Query schema name.
     * @param startTime Query start time.
     * @param duration Query current duration.
     * @param cancellable {@code true} if query can be canceled.
     * @param loc {@code true} if query is local.
     */
    public VisorRunningQuery(long id, String qry, GridCacheQueryType qryType, String schemaName,
        long startTime, long duration,
        boolean cancellable, boolean loc) {
        this.id = id;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.duration = duration;
        this.cancellable = cancellable;
        this.loc = loc;
    }

    /**
     * @return Query ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @return Query txt.
     */
    public String getQuery() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType getQueryType() {
        return qryType;
    }

    /**
     * @return Schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return Query duration.
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean isCancelable() {
        return cancellable;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean isLocal() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(id);
        U.writeString(out, qry);
        U.writeEnum(out, qryType);
        U.writeString(out, schemaName);
        out.writeLong(startTime);
        out.writeLong(duration);
        out.writeBoolean(cancellable);
        out.writeBoolean(loc);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        qry = U.readString(in);
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        schemaName = U.readString(in);
        startTime = in.readLong();
        duration = in.readLong();
        cancellable = in.readBoolean();
        loc = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRunningQuery.class, this);
    }
}
