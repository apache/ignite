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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;

/** Result of {@code RepairResultTask}. */
public class RepairResult {
    /** Keys to repair with corresponding values and versions per nodes. */
    private Map<PartitionKeyVersion, Map<UUID, VersionedValue>> keysToRepair = new HashMap<>();

    /** Repaired keys. */
    private Map<PartitionKeyVersion, RepairMeta> repairedKeys = new HashMap<>();

    /**
     * Default constructor.
     */
    public RepairResult() {
    }

    /**
     * Constructor.
     *
     * @param keysToRepair Keys to repair within next recheck-repair iteration.
     * @param repairedKeys Repaired keys.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public RepairResult(
        Map<PartitionKeyVersion, Map<UUID, VersionedValue>> keysToRepair,
        Map<PartitionKeyVersion, RepairMeta> repairedKeys) {
        this.keysToRepair = keysToRepair;
        this.repairedKeys = repairedKeys;
    }

    /**
     * @return Keys to repair.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<PartitionKeyVersion, Map<UUID, VersionedValue>> keysToRepair() {
        return keysToRepair;
    }

    /**
     * @return Repaired keys.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<PartitionKeyVersion, RepairMeta> repairedKeys() {
        return repairedKeys;
    }
}
