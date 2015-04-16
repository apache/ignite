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

package org.apache.ignite.internal.visor.query;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Task for execute SCAN or SQL query and get first page of results.
 */
@GridInternal
public class VisorQueryTask extends VisorOneNodeTask<VisorQueryArg, IgniteBiTuple<? extends Exception, VisorQueryResultEx>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid,
        VisorTaskArgument<VisorQueryArg> arg) {
        String cache = taskArg.cacheName();

        ClusterNode node;

        UUID nid = taskArg.nodeId();

        IgniteClusterEx cluster = ignite.cluster();

        if (nid == null) {
            ClusterGroup prj = cluster.localNode().isDaemon() ? cluster.forRemotes() : cluster.forDataNodes(cache);

            if (prj.nodes().isEmpty())
                throw new IgniteException("No data nodes for cache: " + escapeName(cache));

            // First try to take local node to avoid network hop.
            node = prj.node(ignite.localNode().id());

            // Take any node from projection.
            if (node == null)
                node = prj.forRandom().node();
        }
        else {
            node = cluster.node(nid);

            if (node == null)
                throw new IgniteException("Node not found: " + nid);
        }

        assert node != null;

        return Collections.singletonMap(job(taskArg), node);
    }

    /** {@inheritDoc} */
    @Override protected VisorQueryJob job(VisorQueryArg arg) {
        return new VisorQueryJob(arg, debug);
    }
}
