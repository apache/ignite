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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Input parameters for checking snapshot partitions consistency task.
 */
public class SnapshotPartitionsVerifyTaskArg extends VisorDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Cache group names to be verified. */
    private Collection<String> grpNames;

    /** The map of distribution of snapshot metadata pieces across the cluster. */
    private Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

    /** Snapshot directory path. */
    private String snpPath;

    /** Default constructor. */
    public SnapshotPartitionsVerifyTaskArg() {
        // No-op.
    }

    /**
     * @param grpNames Cache group names to be verified.
     * @param clusterMetas The map of distribution of snapshot metadata pieces across the cluster.
     * @param snpPath Snapshot directory path.
     */
    public SnapshotPartitionsVerifyTaskArg(
        Collection<String> grpNames,
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        @Nullable String snpPath
    ) {
        this.grpNames = grpNames;
        this.clusterMetas = clusterMetas;
        this.snpPath = snpPath;
    }

    /**
     * @return Cache group names to be verified.
     */
    public Collection<String> cacheGroupNames() {
        return grpNames;
    }

    /**
     * @return The map of distribution of snapshot metadata pieces across the cluster.
     */
    public Map<ClusterNode, List<SnapshotMetadata>> clusterMetadata() {
        return clusterMetas;
    }

    /**
     * @return Snapshot directory path.
     */
    public String snapshotPath() {
        return snpPath;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, grpNames);
        U.writeMap(out, clusterMetas);
        U.writeString(out, snpPath);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        grpNames = U.readCollection(in);
        clusterMetas = U.readMap(in);
        snpPath = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotPartitionsVerifyTaskArg.class, this);
    }
}
