/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * The result of execution snapshot partitions verify task which besides calculating partition hashes of
 * {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
 */
public class SnapshotPartitionsVerifyTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Map of snapshot metadata information found on each cluster node. */
    private Map<ClusterNode, List<SnapshotMetadata>> metas;

    /** Result of cluster nodes partitions comparison. */
    private IdleVerifyResultV2 idleRes;

    /** Default constructor. */
    public SnapshotPartitionsVerifyTaskResult() {
        // No-op.
    }

    /**
     * @param metas Map of snapshot metadata information found on each cluster node.
     * @param idleRes Result of cluster nodes partitions comparison.
     */
    public SnapshotPartitionsVerifyTaskResult(
        Map<ClusterNode, List<SnapshotMetadata>> metas,
        IdleVerifyResultV2 idleRes
    ) {
        this.metas = metas;
        this.idleRes = idleRes;
    }

    /**
     * @return Map of snapshot metadata information found on each cluster node.
     */
    public Map<ClusterNode, List<SnapshotMetadata>> metas() {
        return metas;
    }

    /**
     * @return Result of cluster nodes partitions comparison.
     */
    public IdleVerifyResultV2 idleVerifyResult() {
        return idleRes;
    }

    /**
     * @return Exceptions on nodes.
     */
    public Map<ClusterNode, Exception> exceptions() {
        return idleRes == null ? Collections.emptyMap() : idleRes.exceptions();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, metas);
        out.writeObject(idleRes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        metas = U.readMap(in);
        idleRes = (IdleVerifyResultV2)in.readObject();
    }
}
