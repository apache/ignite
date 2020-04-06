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

import java.util.List;
import org.apache.ignite.IgniteSnapshot;

/**
 * Snapshot features MBean.
 */
@MXBeanDescription("MBean that provides access for snapshot features.")
public interface SnapshotMXBean {
    /**
     * Gets all created snapshots on the cluster.
     *
     * @return List of all known snapshots.
     */
    @MXBeanDescription("List of all known snapshots.")
    public List<String> getSnapshots();

    /**
     * Create the cluster-wide snapshot with given name.
     *
     * @param snpName Snapshot name to created.
     * @see IgniteSnapshot#createSnapshot(String) (String)
     */
    @MXBeanDescription("Create cluster-wide snapshot.")
    public void createSnapshot(@MXBeanParameter(name = "snpName", description = "Snapshot name.") String snpName);
}
