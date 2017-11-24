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

package org.apache.ignite.internal.processors.continuous;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/**
 *
 */
class ContinuousRoutinesInfo {
    /** */
    private Map<UUID, ContinuousRoutineInfo> startedRoutines = new HashMap<>();

    void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(CONTINUOUS_PROC.ordinal()))
            dataBag.addGridCommonData(CONTINUOUS_PROC.ordinal(),
                new ContinuousRoutinesCommonDiscoveryData(new ArrayList<>(startedRoutines.values())));
    }

    void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(CONTINUOUS_PROC.ordinal(),
            new ContinuousRoutinesJoiningNodeDiscoveryData(new ArrayList<>(startedRoutines.values())));
    }

    void addRoutineInfo(ContinuousRoutineInfo info) {
        startedRoutines.put(info.routineId, info);
    }

    boolean routineExists(UUID routineId) {
        return startedRoutines.containsKey(routineId);
    }

    void removeRoutine(UUID routineId) {
        startedRoutines.remove(routineId);
    }

    void removeNodeRoutines(UUID nodeId) {
        for (Iterator<Map.Entry<UUID, ContinuousRoutineInfo>> it = startedRoutines.entrySet().iterator(); it.hasNext();) {
            Map.Entry<UUID, ContinuousRoutineInfo> e = it.next();

            ContinuousRoutineInfo info = e.getValue();

            if (info.autoUnsubscribe && info.srcNodeId.equals(nodeId))
                it.remove();
        }
    }
}
