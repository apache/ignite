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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Input parameters for checking snapshot partitions consistency task.
 */
public class SnapshotPartitionsVerifyTaskArg extends IgniteDataTransferObject {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Cache group names to be verified. */
    @Nullable private Collection<String> grpNames;

    /** The map of distribution of snapshot metadata pieces across the cluster. */
    private Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

    /** Snapshot directory path. */
    @Nullable private String snpPath;

    /** If {@code true} check snapshot integrity. */
    private boolean check;

    /** Incremental snapshot index. */
    private int incIdx;

    /** Default constructor. */
    public SnapshotPartitionsVerifyTaskArg() {
        // No-op.
    }

    /**
     * @param grpNames Cache group names to be verified.
     * @param clusterMetas The map of distribution of snapshot metadata pieces across the cluster.
     * @param snpPath Snapshot directory path.
     * @param incIdx Incremental snapshot index.
     * @param check If {@code true} check snapshot integrity.
     */
    public SnapshotPartitionsVerifyTaskArg(
        @Nullable Collection<String> grpNames,
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        @Nullable String snpPath,
        int incIdx,
        boolean check
    ) {
        this.grpNames = grpNames;
        this.clusterMetas = clusterMetas;
        this.snpPath = snpPath;
        this.incIdx = incIdx;
        this.check = check;
    }

    /**
     * @return Cache group names to be verified.
     */
    @Nullable public Collection<String> cacheGroupNames() {
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
    @Nullable public String snapshotPath() {
        return snpPath;
    }

    /**
     * @return Incremental snapshot index.
     */
    public int incrementIndex() {
        return incIdx;
    }

    /** @return If {@code true} check snapshot integrity. */
    public boolean check() {
        return check;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, grpNames);
        U.writeMap(out, clusterMetas);
        U.writeString(out, snpPath);
        out.writeBoolean(check);
        out.writeInt(incIdx);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        grpNames = U.readCollection(in);
        clusterMetas = U.readMap(in);
        snpPath = U.readString(in);
        check = in.readBoolean();
        incIdx = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotPartitionsVerifyTaskArg.class, this);
    }
}
