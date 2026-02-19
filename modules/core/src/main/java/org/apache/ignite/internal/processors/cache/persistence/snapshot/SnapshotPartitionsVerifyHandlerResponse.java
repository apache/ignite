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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/** */
public class SnapshotPartitionsVerifyHandlerResponse implements Message {
    /** */
    private Map<PartitionKey, PartitionHashRecord> res;

    /** */
    @Order(0)
    private byte[] resBytes;

    /** */
    public SnapshotPartitionsVerifyHandlerResponse() {

    }

    /** */
    public SnapshotPartitionsVerifyHandlerResponse(Map<PartitionKey, PartitionHashRecord> res) {
        this.res = res;
    }

    /** */
    public Map<PartitionKey, PartitionHashRecord> res() {
        return res;
    }

    /** */
    public byte[] resBytes() {
        if (resBytes != null)
            return resBytes;

        try {
            return resBytes = U.marshal(jdk(), res);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public void resBytes(byte[] resBytes) {
        if (resBytes == null)
            return;

        try {
            res = U.unmarshal(jdk(), resBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 527;
    }
}
