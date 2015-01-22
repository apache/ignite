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

package org.apache.ignite.streamer;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;

/**
 * Streamer adapter for event routers.
 */
public abstract class StreamerEventRouterAdapter implements StreamerEventRouter {
    /** {@inheritDoc} */
    @Override public <T> Map<ClusterNode, Collection<T>> route(StreamerContext ctx, String stageName,
        Collection<T> evts) {
        if (evts.size() == 1) {
            ClusterNode route = route(ctx, stageName, F.first(evts));

            if (route == null)
                return null;

            return Collections.singletonMap(route, evts);
        }

        Map<ClusterNode, Collection<T>> map = new GridLeanMap<>();

        for (T e : evts) {
            ClusterNode n = route(ctx, stageName, e);

            if (n == null)
                return null;

            Collection<T> mapped = map.get(n);

            if (mapped == null)
                map.put(n, mapped = new ArrayList<>());

            mapped.add(e);
        }

        return map;
    }
}
