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

package org.apache.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.events.*;

import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Test task writing all events to a stream.
 */
@SuppressWarnings("UnusedDeclaration")
public class PlatformEventsWriteEventTask extends ComputeTaskAdapter<Long, Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        Long ptr) {
        return Collections.singletonMap(new Job(ptr, F.first(subgrid)), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    @SuppressWarnings("deprecation")
    private static class Job extends ComputeJobAdapter {
        /** Grid. */
        @IgniteInstanceResource
        protected transient Ignite ignite;

        /** Stream ptr. */
        private final long ptr;

        private final ClusterNode node;

        /**
         * Constructor.
         *
         * @param ptr Stream ptr.
         */
        public Job(long ptr, ClusterNode node) {
            this.ptr = ptr;
            this.node = node;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            PlatformContext ctx = PlatformUtils.platformContext(ignite);

            try (PlatformMemory mem = ctx.memory().get(ptr)) {
                PlatformOutputStream out = mem.output();
                BinaryRawWriterEx writer = ctx.writer(out);

                int evtType = EventType.EVT_SWAP_SPACE_CLEARED;
                String msg = "msg";
                UUID uuid = new UUID(1, 2);
                IgniteUuid igniteUuid = new IgniteUuid(uuid, 3);

                ctx.writeEvent(writer, new CacheEvent("cacheName", node, node, "msg", evtType, 1, true, 2,
                    igniteUuid, 3, 4, true, 5, true, uuid, "cloClsName", "taskName"));

                //noinspection unchecked
                ctx.writeEvent(writer, new CacheQueryExecutedEvent(node, msg, evtType, "qryType", "cacheName",
                    "clsName", "clause", null, null, null, uuid, "taskName"));

                //noinspection unchecked
                ctx.writeEvent(writer, new CacheQueryReadEvent(node, msg, evtType, "qryType", "cacheName",
                    "clsName", "clause", null, null, null, uuid, "taskName", 1, 2, 3, 4));

                ctx.writeEvent(writer, new CacheRebalancingEvent("cacheName", node, msg, evtType, 1, node, 2, 3));

                ctx.writeEvent(writer, new CheckpointEvent(node, msg, evtType, "cpKey"));

                DiscoveryEvent discoveryEvent = new DiscoveryEvent(node, msg, evtType, node);
                discoveryEvent.topologySnapshot(ignite.cluster().topologyVersion(), ignite.cluster().nodes());
                ctx.writeEvent(writer, discoveryEvent);

                JobEvent jobEvent = new JobEvent(node, msg, evtType);
                jobEvent.jobId(igniteUuid);
                jobEvent.taskClassName("taskClsName");
                jobEvent.taskName("taskName");
                jobEvent.taskNode(node);
                jobEvent.taskSessionId(igniteUuid);
                jobEvent.taskSubjectId(uuid);
                ctx.writeEvent(writer, jobEvent);

                ctx.writeEvent(writer, new SwapSpaceEvent(node, msg, evtType, "space"));

                ctx.writeEvent(writer, new TaskEvent(node, msg, evtType, igniteUuid, "taskName", "taskClsName",
                    true, uuid));

                out.synchronize();
            }

            return true;
        }
    }
}
