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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/**
 *
 */
class ContinuousRoutinesInfo {
    /** */
    private final Map<UUID, ContinuousRoutineInfo> startedRoutines = new HashMap<>();

    /**
     * @param dataBag Discovery data bag.
     */
    void collectGridNodeData(DiscoveryDataBag dataBag) {
        synchronized (startedRoutines) {
            if (!dataBag.commonDataCollectedFor(CONTINUOUS_PROC.ordinal()))
                dataBag.addGridCommonData(CONTINUOUS_PROC.ordinal(),
                    new ContinuousRoutinesCommonDiscoveryData(new ArrayList<>(startedRoutines.values())));
        }
    }

    /**
     * @param dataBag Discovery data bag.
     */
    void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        synchronized (startedRoutines) {
            for (ContinuousRoutineInfo info : startedRoutines.values()) {
                if (info.disconnected)
                    info.sourceNodeId(dataBag.joiningNodeId());
            }

            dataBag.addJoiningNodeData(CONTINUOUS_PROC.ordinal(),
                new ContinuousRoutinesJoiningNodeDiscoveryData(new ArrayList<>(startedRoutines.values())));
        }
    }

    /**
     * @param info Routine info.
     */
    void addRoutineInfo(ContinuousRoutineInfo info) {
        synchronized (startedRoutines) {
            startedRoutines.put(info.routineId, info);
        }
    }

    /**
     * @param routineId Routine ID.
     * @return {@code True} if routine exists.
     */
    boolean routineExists(UUID routineId) {
        synchronized (startedRoutines) {
            return startedRoutines.containsKey(routineId);
        }
    }

    /**
     * @param routineId Routine ID.
     */
    void removeRoutine(UUID routineId) {
        synchronized (startedRoutines) {
            startedRoutines.remove(routineId);
        }
    }

    /**
     * @param locRoutines Routines IDs which can survive reconnect.
     */
    void onClientDisconnected(Collection<UUID> locRoutines) {
        synchronized (startedRoutines) {
            for (Iterator<Map.Entry<UUID, ContinuousRoutineInfo>> it = startedRoutines.entrySet().iterator(); it.hasNext();) {
                Map.Entry<UUID, ContinuousRoutineInfo> e = it.next();

                ContinuousRoutineInfo info = e.getValue();

                if (!locRoutines.contains(info.routineId))
                    it.remove();
                else
                    info.onDisconnected();
            }
        }
    }

    /**
     * Removes all routines with autoUnsubscribe=false started by given node.
     *
     * @param nodeId Node ID.
     */
    void onNodeFail(UUID nodeId) {
        synchronized (startedRoutines) {
            for (Iterator<Map.Entry<UUID, ContinuousRoutineInfo>> it = startedRoutines.entrySet().iterator(); it.hasNext();) {
                Map.Entry<UUID, ContinuousRoutineInfo> e = it.next();

                ContinuousRoutineInfo info = e.getValue();

                if (info.autoUnsubscribe && info.srcNodeId.equals(nodeId))
                    it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinuousRoutinesInfo.class, this);
    }
}
