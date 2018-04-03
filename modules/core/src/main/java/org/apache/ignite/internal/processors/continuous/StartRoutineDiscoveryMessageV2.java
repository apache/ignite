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

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StartRoutineDiscoveryMessageV2 extends AbstractContinuousMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int KEEP_BINARY_FLAG = 0x01;

    /** */
    private final StartRequestDataV2 startReqData;

    /** Flags. */
    private int flags;

    /**
     * @param routineId Routine id.
     * @param startReqData Start request data.
     * @param keepBinary Keep binary flag.
     */
    StartRoutineDiscoveryMessageV2(UUID routineId, StartRequestDataV2 startReqData, boolean keepBinary) {
        super(routineId);

        this.startReqData = startReqData;

        if (keepBinary)
            flags |= KEEP_BINARY_FLAG;
    }

    /**
     * @return Start request data.
     */
    public StartRequestDataV2 startRequestData() {
        return startReqData;
    }

    /**
     * @return {@code True} if keep binary flag was set on continuous handler.
     */
    public boolean keepBinary() {
        return (flags & KEEP_BINARY_FLAG) != 0;
    }

    /** {@inheritDoc} */
    @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartRoutineDiscoveryMessageV2.class, this, "routineId", routineId());
    }
}
