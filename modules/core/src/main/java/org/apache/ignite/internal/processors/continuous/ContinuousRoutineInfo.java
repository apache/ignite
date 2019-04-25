/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.continuous;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class ContinuousRoutineInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    UUID srcNodeId;

    /** */
    final UUID routineId;

    /** */
    final byte[] hnd;

    /** */
    final byte[] nodeFilter;

    /** */
    final int bufSize;

    /** */
    final long interval;

    /** */
    final boolean autoUnsubscribe;

    /** */
    transient boolean disconnected;

    /**
     * @param srcNodeId Source node ID.
     * @param routineId Routine ID.
     * @param hnd Marshalled handler.
     * @param nodeFilter Marshalled node filter.
     * @param bufSize Handler buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Auto unsubscribe flag.
     */
    ContinuousRoutineInfo(
        UUID srcNodeId,
        UUID routineId,
        byte[] hnd,
        byte[] nodeFilter,
        int bufSize,
        long interval,
        boolean autoUnsubscribe)
    {
        this.srcNodeId = srcNodeId;
        this.routineId = routineId;
        this.hnd = hnd;
        this.nodeFilter = nodeFilter;
        this.bufSize = bufSize;
        this.interval = interval;
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /**
     * @param srcNodeId Source node ID.
     */
    void sourceNodeId(UUID srcNodeId) {
        this.srcNodeId = srcNodeId;
    }

    /**
     *
     */
    void onDisconnected() {
        disconnected = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinuousRoutineInfo.class, this);
    }
}
