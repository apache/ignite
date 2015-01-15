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

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task used to test portable affinity key.
 */
public class GridClientTestPortableAffinityKeyTask extends ComputeTaskAdapter<Object, Boolean> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> clusterNodes,
        @Nullable final Object arg) throws IgniteCheckedException {
        for (ClusterNode node : clusterNodes) {
            if (node.isLocal())
                return Collections.singletonMap(new ComputeJobAdapter() {
                    @Override public Object execute() throws IgniteCheckedException {
                        return executeJob(arg);
                    }
                }, node);
        }

        throw new IgniteCheckedException("Failed to find local node in task topology: " + clusterNodes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return results.get(0).getData();
    }

    /**
     * @param arg Argument.
     * @return Execution result.
     * @throws IgniteCheckedException If failed.
     */
     protected Boolean executeJob(Object arg) throws IgniteCheckedException {
        Collection args = (Collection)arg;

        Iterator<Object> it = args.iterator();

        assert args.size() == 3 : args.size();

        PortableObject obj = (PortableObject)it.next();

        String cacheName = (String)it.next();

        String expAffKey = (String)it.next();

        Object affKey = ignite.cache(cacheName).affinity().affinityKey(obj);

        if (!expAffKey.equals(affKey))
            throw new IgniteCheckedException("Unexpected affinity key: " + affKey);

        if (!ignite.cache(cacheName).affinity().mapKeyToNode(obj).isLocal())
            throw new IgniteCheckedException("Job is not run on primary node.");

        return true;
    }
}
