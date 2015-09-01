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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Task that returns collection of cache data nodes IDs.
 */
@GridInternal
public class VisorCacheNodesTask extends VisorOneNodeTask<String, Collection<UUID>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheNodesJob job(String arg) {
        return new VisorCacheNodesJob(arg, debug);
    }

    /**
     * Job that collects cluster group for specified cache.
     */
    private static class VisorCacheNodesJob extends VisorJob<String, Collection<UUID>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param cacheName Cache name to clear.
         * @param debug Debug flag.
         */
        private VisorCacheNodesJob(String cacheName, boolean debug) {
            super(cacheName, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<UUID> run(String cacheName) {
            Collection<ClusterNode> nodes = ignite.cluster().forDataNodes(cacheName).nodes();

            Collection<UUID> res = new ArrayList<>(nodes.size());

            for (ClusterNode node : nodes)
                res.add(node.id());

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheNodesJob.class, this);
        }
    }
}