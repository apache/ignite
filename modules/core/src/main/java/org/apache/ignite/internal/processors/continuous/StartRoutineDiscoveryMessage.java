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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationException;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Discovery message used for Continuous Query registration.
 */
public class StartRoutineDiscoveryMessage extends AbstractContinuousMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final StartRequestData startReqData;

    /** */
    // Initilized here as well to preserve compatibility with previous versions
    private Map<UUID, IgniteCheckedException> errs = new HashMap<>();

    /** */
    private Map<Integer, T2<Long, Long>> updateCntrs;

    /** */
    private Map<UUID, Map<Integer, T2<Long, Long>>> updateCntrsPerNode;

    /** Keep binary flag. */
    private boolean keepBinary;

    /** */
    private transient ClassNotFoundException deserEx;

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
        if (errs == null)
            errs = new HashMap<>();

        errs.put(nodeId, e);
    }

    /**
     * @param cntrs Update counters.
     */
    private void addUpdateCounters(Map<Integer, T2<Long, Long>> cntrs) {
        if (updateCntrs == null)
            updateCntrs = new HashMap<>();

        for (Map.Entry<Integer, T2<Long, Long>> e : cntrs.entrySet()) {
            T2<Long, Long> cntr0 = updateCntrs.get(e.getKey());
            T2<Long, Long> cntr1 = e.getValue();

            if (cntr0 == null || cntr1.get2() > cntr0.get2())
                updateCntrs.put(e.getKey(), cntr1);
        }
    }

    /**
     * @param nodeId Local node ID.
     * @param cntrs Update counters.
     */
    public void addUpdateCounters(UUID nodeId, Map<Integer, T2<Long, Long>> cntrs) {
        addUpdateCounters(cntrs);

        if (updateCntrsPerNode == null)
            updateCntrsPerNode = new HashMap<>();

        Map<Integer, T2<Long, Long>> old = updateCntrsPerNode.put(nodeId, cntrs);

        assert old == null : old;
    }

    /**
     * @return Errs.
     */
    public Map<UUID, IgniteCheckedException> errs() {
        return errs != null ? errs : Collections.emptyMap();
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
        return new StartRoutineAckDiscoveryMessage(routineId, errs(), updateCntrs, updateCntrsPerNode);
    }

    /** */
    private void readObject(ObjectInputStream in) throws IOException {
        // Override default serialization in order to tolerate missing classes exceptions (e.g. remote filter class).
        // We need this means because CQ registration process assumes that an "ack message" will be sent.
        try {
            in.defaultReadObject();
        }
        catch (ClassNotFoundException e) {
            deserEx = e;

            throw new IncompleteDeserializationException(this);
        }
    }

    /**
     * @return Exception occurred during deserialization.
     */
    @Nullable public ClassNotFoundException deserializationException() {
        return deserEx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartRoutineDiscoveryMessage.class, this, "routineId", routineId());
    }
}
