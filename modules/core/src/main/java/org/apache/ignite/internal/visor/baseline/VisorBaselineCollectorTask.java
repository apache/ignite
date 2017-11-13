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

package org.apache.ignite.internal.visor.baseline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will collect baseline topology information.
 */
@GridInternal
public class VisorBaselineCollectorTask extends VisorOneNodeTask<Void, VisorBaselineCollectorTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorBaselineCollectorJob job(Void arg) {
        return new VisorBaselineCollectorJob(arg, debug);
    }

    /**
     * Job that will collect baseline topology information.
     */
    private static class VisorBaselineCollectorJob extends VisorJob<Void, VisorBaselineCollectorTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorBaselineCollectorJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorBaselineCollectorTaskResult run(@Nullable Void arg) throws IgniteException {
            IgniteClusterEx cluster = ignite.cluster();

            Collection<BaselineNode> baseline = cluster.currentBaselineTopology();

            if (baseline == null)
                return new VisorBaselineCollectorTaskResult(cluster.topologyVersion(), null,
                    cluster.forServers().nodes());

            Set<Object> baselineIDs = new HashSet<>(baseline.size());

            for (BaselineNode node : baseline)
                baselineIDs.add(node.consistentId());

            Collection<ClusterNode> srvrs = cluster.forServers().nodes();

            Collection<ClusterNode> others = new ArrayList<>();

            for (ClusterNode server : srvrs) {
                if (!baselineIDs.contains(server.consistentId()))
                    others.add(server);
            }

            return new VisorBaselineCollectorTaskResult(cluster.topologyVersion(), baseline, others);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBaselineCollectorJob.class, this);
        }
    }
}
