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
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Arguments of task for cancel SCAN query.
 */
@GridInternal
public class VisorScanQueryCancelTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query ID to cancel. */
    private long qryId;

    /** Cache name. */
    private String cacheName;

    /** Query originating node to cancel. */
    private UUID originNodeId;

    /**
     * Default constructor.
     */
    public VisorScanQueryCancelTaskArg() {
        // No-op.
    }

    /**
     * @param qryId Query ID to cancel.
     */
    public VisorScanQueryCancelTaskArg(UUID originNodeId, String cacheName, long qryId) {
        this.originNodeId = originNodeId;
        this.cacheName = cacheName;
        this.qryId = qryId;
    }

    /**
     * @return Query ID to cancel.
     */
    public long getQueryId() {
        return qryId;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Query originating node to cancel.
     */
    public UUID getOriginNodeId() {
        return originNodeId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(qryId);
        U.writeString(out, cacheName);
        U.writeUuid(out, originNodeId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        qryId = in.readLong();
        cacheName = U.readString(in);
        originNodeId = U.readUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorScanQueryCancelTaskArg.class, this);
    }
}
