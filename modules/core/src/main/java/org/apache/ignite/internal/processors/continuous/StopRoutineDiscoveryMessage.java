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

import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class StopRoutineDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Routine ID. */
    private final UUID routineId;

    /**
     * @param routineId Routine id.
     */
    public StopRoutineDiscoveryMessage(UUID routineId) {
        this.routineId = routineId;
    }

    /** {@inheritDoc} */
    @Override public boolean forwardMinorVersion() {
        return false;
    }

    /**
     * @return Routine ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage newMessageOnRingEnd() {
        return new StopRoutineAckDiscoveryMessage(routineId);
    }
}
