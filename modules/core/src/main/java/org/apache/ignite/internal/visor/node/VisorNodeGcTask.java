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

package org.apache.ignite.internal.visor.node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorNodeGcTask extends VisorMultiNodeTask<Void, Map<UUID, IgniteBiTuple<Long, Long>>,
    IgniteBiTuple<Long, Long>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodeGcJob job(Void arg) {
        return new VisorNodeGcJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteBiTuple<Long, Long>> reduce0(List<ComputeJobResult> results) {
        Map<UUID, IgniteBiTuple<Long, Long>> total = new HashMap<>();

        for (ComputeJobResult res : results) {
            IgniteBiTuple<Long, Long> jobRes = res.getData();

            total.put(res.getNode().id(), jobRes);
        }

        return total;
    }

    /** Job that perform GC on node. */
    private static class VisorNodeGcJob extends VisorJob<Void, IgniteBiTuple<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Formal task argument.
         * @param debug Debug flag.
         */
        private VisorNodeGcJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Long, Long> run(Void arg) {
            ClusterNode locNode = ignite.localNode();

            long before = freeHeap(locNode);

            System.gc();

            return new IgniteBiTuple<>(before, freeHeap(locNode));
        }

        /**
         * @param node Node.
         * @return Current free heap.
         */
        private long freeHeap(ClusterNode node) {
            final ClusterMetrics m = node.metrics();

            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodeGcJob.class, this);
        }
    }
}