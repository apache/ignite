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

import java.io.Serializable;
import java.util.UUID;

/**
 * Snapshot restore operation details.
 */
public class SnapshotRestoreStatusDetails implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Message of the exception that led to an interruption of the process. */
    private String errMsg;

    /** Request ID. */
    private UUID reqId;

    /** Operation start time. */
    private long startTime;

    /** Operation end time. */
    private long endTime;

    /** Names of the restored cache groups. */
    private String cacheGrpNames;

    /** Number of processed (copied) partitions. */
    private long processedParts;

    /** Total number of partitions to be restored. */
    private long totalParts;

    /** Size of processed (copied) partitions in bytes. */
    private long processedBytes;

    /** Total size of the partitions to be restored in bytes. */
    private long totalBytes;

    /** Default constructor. */
    public SnapshotRestoreStatusDetails() {
        // No-op.
    }

    /**
     * @param reqId Request ID.
     * @param startTime Operation start time.
     * @param endTime Operation end time.
     * @param errMsg Message of the exception that led to an interruption of the process.
     * @param processedParts Number of processed (copied) partitions.
     * @param processedBytes Size of processed (copied) partitions in bytes.
     * @param cacheGrpNames Names of the restored cache groups.
     * @param totalParts Total number of partitions to be restored.
     * @param totalBytes Total size of the partitions to be restored in bytes.
     */
    public SnapshotRestoreStatusDetails(UUID reqId, long startTime, long endTime, String errMsg,
        long processedParts, long processedBytes, String cacheGrpNames, long totalParts, long totalBytes) {
        this.reqId = reqId;
        this.errMsg = errMsg;
        this.startTime = startTime;
        this.cacheGrpNames = cacheGrpNames;
        this.processedParts = processedParts;
        this.processedBytes = processedBytes;
        this.totalParts = totalParts;
        this.totalBytes = totalBytes;
        this.endTime = endTime;
    }

    /** @return Message of the exception that led to an interruption of the process. */
    public String errorMessage() {
        return errMsg;
    }

    /** @return Request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Operation start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Operation end time. */
    public long endTime() {
        return endTime;
    }

    /** @return Names of the restored cache groups. */
    public String cacheGroupNames() {
        return cacheGrpNames;
    }

    /** @return Number of processed (copied) partitions. */
    public long processedParts() {
        return processedParts;
    }

    /** @return Size of processed (copied) partitions in bytes. */
    public long processedBytes() {
        return processedBytes;
    }

    /** @return Total number of partitions to be restored. */
    public long totalParts() {
        return totalParts;
    }

    /** @return Total size of the partitions to be restored in bytes. */
    public long totalBytes() {
        return totalBytes;
    }
}
