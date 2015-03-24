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

package org.apache.ignite.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridCacheDeploymentSelfTest}.
 */
public class CacheDeploymentTestTask1 extends ComputeTaskAdapter<ClusterNode, Object> {
    /** Number of puts. */
    private static final int PUT_CNT = 100;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable ClusterNode node) {
        return F.asMap(
            new ComputeJobAdapter() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object execute() {
                    X.println("Executing CacheDeploymentTestTask1 job on node " +
                        ignite.cluster().localNode().id());

                    IgniteCache<String, CacheDeploymentTestValue> cache = ignite.cache(null);

                    for (int i = 0; i < PUT_CNT; i++)
                        cache.put("1" + i, new CacheDeploymentTestValue());

                    return null;
                }
            },
            node
        );
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return null;
    }
}
