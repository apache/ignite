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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;

/**
 * While processing cache partition delta file it can contains a batch o pages
 * which is not related to the current running process (e.g. newly allocated page which
 * is written to the end of partition file and which is not belongs to the previously
 * copied partiton file by offset).
 */
public interface BackupProcessTask {
    /**
     * @param grpPartId Cache group and partition pair identifiers.
     * @param file A representation of partiton file.
     * @param size Partiton size in bytes to handle.
     * @throws IgniteCheckedException If fails.
     */
    public void handlePartition(
        GroupPartitionId grpPartId,
        File file,
        long size
    ) throws IgniteCheckedException;

    /**
     * @param grpPartId Cache group and partition pair identifiers.
     * @param file A representation of partiton file.
     * @param offset Start point offset.
     * @param size Size of delta to handle.
     * @throws IgniteCheckedException If fails.
     */
    public void handleDelta(
        GroupPartitionId grpPartId,
        File file,
        long offset,
        long size
    ) throws IgniteCheckedException;
}
