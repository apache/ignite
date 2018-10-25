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

package org.apache.ignite.internal.processors.security;

import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains node id into the thread local variable. All methods ignore passed local node id.
 */
public class CurrentRemoteInitiatorIdentifier {
    /** Initiator. */
    private final ThreadLocal<UUID> initiator = new ThreadLocal<>();

    /**
     * Set initiator node id. If passed node id is local node id then it'll be ignored.
     *
     * @param ctx Kernal context.
     * @param nodeId Node id.
     * @return True if id was set.
     */
    public boolean set(GridKernalContext ctx, UUID nodeId) {
        if (!ctx.localNodeId().equals(nodeId)) {
            UUID oldNodeId = initiator.get();

            assert oldNodeId == null : "oldNodeId=" + oldNodeId;

            initiator.set(nodeId);

            return true;
        }

        return false;
    }

    /**
     * Getting current initiator node id.
     *
     * @return Node id.
     */
    @Nullable public UUID get() {
        return initiator.get();
    }

    /**
     * Remove node id if passed id isn't local node id.
     *
     * @param ctx Kernal context.
     * @param nodeId Node's id.
     */
    public void remove(GridKernalContext ctx, UUID nodeId) {
        if (!ctx.localNodeId().equals(nodeId))
            initiator.remove();
    }
}
