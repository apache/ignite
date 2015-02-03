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

package org.apache.ignite.streamer.router;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Random router. Routes event to random node.
 */
public class StreamerRandomEventRouter extends StreamerEventRouterAdapter {
    /** Optional predicates to exclude nodes from routing. */
    private IgnitePredicate<ClusterNode>[] predicates;

    /**
     * Empty constructor for spring.
     */
    public StreamerRandomEventRouter() {
        this((IgnitePredicate<ClusterNode>[])null);
    }

    /**
     * Constructs random event router with optional set of filters to apply to streamer projection.
     *
     * @param predicates Node predicates.
     */
    public StreamerRandomEventRouter(@Nullable IgnitePredicate<ClusterNode>... predicates) {
        this.predicates = predicates;
    }

    /**
     * Constructs random event router with optional set of filters to apply to streamer projection.
     *
     * @param predicates Node predicates.
     */
    @SuppressWarnings("unchecked")
    public StreamerRandomEventRouter(Collection<IgnitePredicate<ClusterNode>> predicates) {
        if (!F.isEmpty(predicates)) {
            this.predicates = new IgnitePredicate[predicates.size()];

            predicates.toArray(this.predicates);
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode route(StreamerContext ctx, String stageName, Object evt) {
        Collection<ClusterNode> nodes = F.view(ctx.projection().nodes(), predicates);

        if (F.isEmpty(nodes))
            return null;

        int idx = ThreadLocalRandom8.current().nextInt(nodes.size());

        int i = 0;

        Iterator<ClusterNode> iter = nodes.iterator();

        while (true) {
            if (!iter.hasNext())
                iter = nodes.iterator();

            ClusterNode node = iter.next();

            if (idx == i++)
                return node;
        }
    }
}
