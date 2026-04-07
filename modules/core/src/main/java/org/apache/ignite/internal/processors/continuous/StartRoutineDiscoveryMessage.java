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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Discovery message used for Continuous Query registration.
 */
public class StartRoutineDiscoveryMessage extends AbstractContinuousMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    StartRequestData startReqData;

    /** */
    @Order(1)
    Map<UUID, ErrorMessage> errs = new HashMap<>();

    /** */
    @Order(2)
    Map<Integer, Long> updateCntrs;

    /** */
    @Order(3)
    Map<UUID, Map<Integer, Long>> updateCntrsPerNode;

    /**
     * @param routineId Routine id.
     * @param startReqData Start request data.
     */
    public StartRoutineDiscoveryMessage(UUID routineId, StartRequestData startReqData) {
        super(routineId);

        this.startReqData = startReqData;
    }

    /** */
    public StartRoutineDiscoveryMessage() {}

    /**
     * @return Start request data.
     */
    public StartRequestData startRequestData() {
        return startReqData;
    }

    /**
     * @param nodeId Node id.
     * @param e Exception.
     */
    public void addError(UUID nodeId, IgniteCheckedException e) {
        if (errs == null)
            errs = new HashMap<>();

        errs.put(nodeId, new ErrorMessage(e));
    }

    /**
     * @param cntrs Update counters.
     */
    private void addUpdateCounters(Map<Integer, Long> cntrs) {
        if (updateCntrs == null)
            updateCntrs = new HashMap<>();

        for (Map.Entry<Integer, Long> e : cntrs.entrySet()) {
            Long cntr0 = updateCntrs.get(e.getKey());
            Long cntr1 = e.getValue();

            if (cntr0 == null || cntr1 > cntr0)
                updateCntrs.put(e.getKey(), cntr1);
        }
    }

    /**
     * @param nodeId Local node ID.
     * @param cntrs Update counters.
     */
    public void addUpdateCounters(UUID nodeId, Map<Integer, Long> cntrs) {
        addUpdateCounters(cntrs);

        if (updateCntrsPerNode == null)
            updateCntrsPerNode = new HashMap<>();

        Map<Integer, Long> old = updateCntrsPerNode.put(nodeId, cntrs);

        assert old == null : old;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public DiscoveryCustomMessage ackMessage() {
        return new StartRoutineAckDiscoveryMessage(routineId, errs, updateCntrs, updateCntrsPerNode);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartRoutineDiscoveryMessage.class, this, "routineId", routineId());
    }
}
