/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
