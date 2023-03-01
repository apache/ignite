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

package org.apache.ignite.mxbean;

import java.util.Collection;
import org.apache.ignite.IgniteSnapshot;

/**
 * Snapshot features MBean.
 */
@MXBeanDescription("MBean that provides access for snapshot features.")
public interface SnapshotMXBean {
    /**
     * Create the cluster-wide snapshot with given name asynchronously.
     *
     * @param snpName Snapshot name to be created.
     * @param snpPath Snapshot directory path.
     * @see IgniteSnapshot#createSnapshot(String) (String)
     */
    @MXBeanDescription("Create cluster-wide snapshot.")
    public void createSnapshot(
        @MXBeanParameter(name = "snpName", description = "Snapshot name.")
            String snpName,
        @MXBeanParameter(name = "snpPath", description = "Optional snapshot directory path.")
            String snpPath
    );

    /**
     * Create the cluster-wide incremental snapshot for the given base snapshot.
     *
     * @param fullSnapshot Full snapshot name to attach incremental snapshot to.
     * @param fullSnapshotPath Full snapshot directory path.
     * @see IgniteSnapshot#createSnapshot(String)
     * @see IgniteSnapshot#createIncrementalSnapshot(String)
     */
    public void createIncrementalSnapshot(
        @MXBeanParameter(name = "fullSnapshot", description = "Snapshot name.")
            String fullSnapshot,
        @MXBeanParameter(name = "fullSnapshotPath", description = "Optional snapshot directory path.")
            String fullSnapshotPath
    );

    /**
     * Cancel previously started snapshot operation on the node initiator.
     *
     * @param snpName Snapshot name to cancel.
     * @deprecated Use {@link #cancelSnapshotOperation(String)} instead.
     */
    @MXBeanDescription("Cancel started cluster-wide snapshot on the node initiator.")
    @Deprecated
    public void cancelSnapshot(@MXBeanParameter(name = "snpName", description = "Snapshot name.") String snpName);

    /**
     * Cancel previously started snapshot operation.
     *
     * @param reqId Snapshot operation request ID.
     */
    @MXBeanDescription("Cancel started cluster-wide snapshot operation.")
    public void cancelSnapshotOperation(@MXBeanParameter(name = "requestId", description = "Snapshot operation request ID.") String reqId);

    /**
     * Restore cluster-wide snapshot.
     *
     * @param name Snapshot name.
     * @param path Snapshot directory path.
     * @param cacheGroupNames Optional comma-separated list of cache group names.
     * @see IgniteSnapshot#restoreSnapshot(String, Collection)
     */
    @MXBeanDescription("Restore cluster-wide snapshot.")
    public void restoreSnapshot(
        @MXBeanParameter(name = "snpName", description = "Snapshot name.")
            String name,
        @MXBeanParameter(name = "snpPath", description = "Optional snapshot directory path.")
            String path,
        @MXBeanParameter(name = "cacheGroupNames", description = "Optional comma-separated list of cache group names.")
            String cacheGroupNames
    );

    /**
     * Restore cluster-wide incremental snapshot.
     *
     * @param name Snapshot name.
     * @param path Snapshot directory path.
     * @param cacheGroupNames Optional comma-separated list of cache group names.
     * @param incIdx Incremental snapshot index.
     * @see IgniteSnapshot#restoreSnapshot(String, Collection)
     */
    @MXBeanDescription("Restore cluster-wide incremental snapshot.")
    public void restoreIncrementalSnapshot(
        @MXBeanParameter(name = "snpName", description = "Snapshot name.")
            String name,
        @MXBeanParameter(name = "snpPath", description = "Optional snapshot directory path.")
            String path,
        @MXBeanParameter(name = "cacheGroupNames", description = "Optional comma-separated list of cache group names.")
            String cacheGroupNames,
        @MXBeanParameter(name = "incIdx", description = "Incremental snapshot index.")
            int incIdx
    );

    /**
     * Cancel previously started snapshot restore operation.
     *
     * @param name Snapshot name.
     * @see IgniteSnapshot#cancelSnapshotRestore(String)
     * @deprecated Use {@link #cancelSnapshotOperation(String)} instead.
     */
    @MXBeanDescription("Cancel previously started snapshot restore operation.")
    @Deprecated
    public void cancelSnapshotRestore(@MXBeanParameter(name = "snpName", description = "Snapshot name.") String name);

    /**
     * Get the status of the current snapshot operation in the cluster.
     *
     * @return The status of a current snapshot operation in the cluster.
     */
    @MXBeanDescription("The status of a current snapshot operation in the cluster.")
    public String status();
}
