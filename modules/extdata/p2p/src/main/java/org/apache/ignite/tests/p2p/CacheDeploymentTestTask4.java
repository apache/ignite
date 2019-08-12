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

import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Test task for {@code GridCacheDeploymentSelfTest}.
 */
public class CacheDeploymentTestTask4 extends ComputeTaskAdapter<T2<ClusterNode, String>, Object> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable T2<ClusterNode, String> tup) {
        final String val = tup.getValue();

        return F.asMap(
                new ComputeJobAdapter() {
                    private static final long serialVersionUID = 0L;

                    /** Ignite instance. */
                    @IgniteInstanceResource
                    private Ignite ignite;

                    /** {@inheritDoc} */
                    @Override public Object execute() {
                        X.println("Executing CacheDeploymentTestTask4 job on node " +
                                ignite.cluster().localNode().id());

                        try {
                            if (val != null)
                                ignite.cache("default").put(val, Val.getVal());
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        return null;
                    }
                },
                tup.get1()
        );
    }

    /** */
    public static class Val {
        /** */
        private static Object getVal() {
            return new CacheDeploymentTestValue();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return null;
    }
}
