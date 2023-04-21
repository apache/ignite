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

package org.apache.ignite.internal.management.kill;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class KillScanCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Originating node id")
    private UUID originNodeId;

    /** */
    @Positional
    @Argument(description = "Cache name")
    private String cacheName;

    /** */
    @Positional
    @Argument(description = "Query identifier")
    private long queryId;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, originNodeId);
        U.writeString(out, cacheName);
        out.writeLong(queryId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        originNodeId = U.readUuid(in);
        cacheName = U.readString(in);
        queryId = in.readLong();
    }

    /** */
    public UUID getOriginNodeId() {
        return originNodeId;
    }

    /** */
    public void setOriginNodeId(UUID originNodeId) {
        this.originNodeId = originNodeId;
    }

    /** */
    public String getCacheName() {
        return cacheName;
    }

    /** */
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public long getQueryId() {
        return queryId;
    }

    /** */
    public void setQueryId(long queryId) {
        this.queryId = queryId;
    }
}
