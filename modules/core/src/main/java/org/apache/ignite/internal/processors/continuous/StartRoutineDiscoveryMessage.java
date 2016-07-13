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
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StartRoutineDiscoveryMessage extends AbstractContinuousMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final StartRequestData startReqData;

    /** */
    private final Map<UUID, IgniteCheckedException> errs = new HashMap<>();

    /** */
    private Map<Integer, Long> updateCntrs;

    /** */
    private Map<UUID, Map<Integer, Long>> updateCntrsPerNode;

    /** Keep binary flag. */
    private boolean keepBinary;

    /**
     * @param routineId Routine id.
     * @param startReqData Start request data.
     */
    public StartRoutineDiscoveryMessage(UUID routineId, StartRequestData startReqData, boolean keepBinary) {
        super(routineId);

        this.startReqData = startReqData;
        this.keepBinary = keepBinary;
    }

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
        errs.put(nodeId, e);
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

    /**
     * @return Errs.
     */
    public Map<UUID, IgniteCheckedException> errs() {
        return errs;
    }

    /**
     * @return {@code True} if keep binary flag was set on continuous handler.
     */
    public boolean keepBinary() {
        return keepBinary;
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