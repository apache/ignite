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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.Map;

/**
 * Result of a checkpint search and reservation.
 */
public class CheckpointHistoryResult {

    /**
     * Map (groupId : Map (partitionId, earliest valid checkpoint to history search)).
     */
    private final Map<Integer, Map<Integer, CheckpointEntry>> earliestValidCheckpoints;

    /** Reserved checkpoint. */
    private final CheckpointEntry reservedCheckpoint;

    /**
     * Constructor.
     *
     * @param earliestValidCheckpoints Map (groupId : Map (partitionId, earliest valid checkpoint to history search)).
     * @param reservedCheckpoint Reserved checkpoint.
     */
    public CheckpointHistoryResult(
        Map<Integer, Map<Integer, CheckpointEntry>> earliestValidCheckpoints,
        CheckpointEntry reservedCheckpoint) {
        this.earliestValidCheckpoints = earliestValidCheckpoints;
        this.reservedCheckpoint = reservedCheckpoint;
    }

    /**
     * @return Map (groupId : Map (partitionId, earliest valid checkpoint to history search)).
     */
    public Map<Integer, Map<Integer, CheckpointEntry>> earliestValidCheckpoints() {
        return earliestValidCheckpoints;
    }

    /**
     * @return Reserved checkpoint.
     */
    public CheckpointEntry reservedCheckoint() {
        return reservedCheckpoint;
    }
}

