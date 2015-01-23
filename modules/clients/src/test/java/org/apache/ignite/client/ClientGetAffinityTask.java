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

package org.apache.ignite.client;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

import static org.apache.ignite.compute.ComputeJobResultPolicy.*;

/**
 * Get affinity for task argument.
 */
public class ClientGetAffinityTask extends TaskSingleJobSplitAdapter<String, Integer> {
    /** Grid. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, String arg) throws IgniteCheckedException {
        A.notNull(arg, "task argument");

        String[] split = arg.split(":", 2);

        A.ensure(split.length == 2, "Task argument should have format 'cacheName:affinityKey'.");

        String cacheName = split[0];
        String affKey = split[1];

        if ("null".equals(cacheName))
            cacheName = null;

        ClusterNode node = ignite.cluster().mapKeyToNode(cacheName, affKey);

        return node.id().toString();
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteCheckedException {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }
}
