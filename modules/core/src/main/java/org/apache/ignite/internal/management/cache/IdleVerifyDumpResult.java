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
package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Encapsulates result of {@link VerifyBackupPartitionsDumpTask}.
 */
public class IdleVerifyDumpResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cluster hashes. */
    private Map<PartitionKey, List<PartitionHashRecord>> clusterHashes;

    /**
     * @param clusterHashes Cluster hashes.
     */
    public IdleVerifyDumpResult(Map<PartitionKey, List<PartitionHashRecord>> clusterHashes) {
        this.clusterHashes = clusterHashes;
    }

    /**
     * Default constructor for Externalizable.
     */
    public IdleVerifyDumpResult() {
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, clusterHashes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        clusterHashes = U.readLinkedMap(in);
    }

    /**
     * @return Cluster hashes.
     */
    public Map<PartitionKey, List<PartitionHashRecord>> clusterHashes() {
        return clusterHashes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyDumpResult.class, this);
    }
}
