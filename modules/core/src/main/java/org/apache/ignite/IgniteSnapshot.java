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

package org.apache.ignite;

import org.apache.ignite.lang.IgniteFuture;

/**
 * This interface provides functionality for creating cluster-wide cache data snapshots.
 * <p>
 * Current limitations:
 * <ul>
 * <li>Snapshot will trigger PME (partition map exchange) to run itself.</li>
 * <li>Snapshot will be taken from all registered persistence caches to
 * grantee data consistency between them.</li>
 * <li>Snapshot must be resorted manually on the switched off cluster by copying data
 * to the working directory on each cluster node.</li>
 * </ul>
 */
public interface IgniteSnapshot {
    /**
     * Create a consistent copy of all persistence cache groups from the whole cluster.
     *
     * @param name Snapshot unique name which satisfies the following name pattern [a-zA-Z0-9_].
     * @return Future which will be completed when a process ends.
     */
    public IgniteFuture<Void> createSnapshot(String name);

    /**
     * Cancel running snapshot operation. All intermediate results of cancelled snapshot operation will be deleted.
     * If snapshot already created this command will have no effect.
     *
     * @param name Snapshot name to cancel.
     * @return Future which will be completed when cancel operation finished.
     */
    public IgniteFuture<Void> cancelSnapshot(String name);
}
