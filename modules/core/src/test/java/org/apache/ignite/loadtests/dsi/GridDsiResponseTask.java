/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.dsi;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.T3;

/**
 * Adapter to be used by client.
 */
public class GridDsiResponseTask extends ComputeTaskSplitAdapter<GridDsiMessage, T3<Long, Integer, Integer>> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int arg0, GridDsiMessage msg) {
        return Collections.singletonList(new GridDsiPerfJob(msg));
    }

    /** {@inheritDoc} */
    @Override public T3<Long, Integer, Integer> reduce(List<ComputeJobResult> results) {
        assert results.size() == 1;

        return results.get(0).getData();
    }
}