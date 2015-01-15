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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.cluster.*;
import org.apache.ignite.streamer.*;

import java.util.*;

/**
 * Test router.
 */
class GridTestStreamerEventRouter extends StreamerEventRouterAdapter {
    /** Route table. */
    private Map<String, UUID> routeTbl = new HashMap<>();

    /**
     * @param stageName Stage name.
     * @param nodeId Node id.
     */
    public void put(String stageName, UUID nodeId) {
        routeTbl.put(stageName, nodeId);
    }

    /** {@inheritDoc} */
    @Override public <T> ClusterNode route(StreamerContext ctx, String stageName, T evt) {
        UUID nodeId = routeTbl.get(stageName);

        if (nodeId == null)
            return null;

        return ctx.projection().node(nodeId);
    }
}
