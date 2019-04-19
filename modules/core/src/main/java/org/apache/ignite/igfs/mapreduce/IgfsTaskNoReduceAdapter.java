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

package org.apache.ignite.igfs.mapreduce;

import java.util.List;
import org.apache.ignite.compute.ComputeJobResult;

/**
 * Convenient {@link IgfsTask} adapter with empty reduce step. Use this adapter in case you are not interested in
 * results returned by jobs.
 */
public abstract class IgfsTaskNoReduceAdapter<T, R> extends IgfsTask<T, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default implementation which will ignore all results sent from execution nodes.
     *
     * @param results Received results of broadcasted remote executions. Note that if task class has
     *      {@link org.apache.ignite.compute.ComputeTaskNoResultCache} annotation, then this list will be empty.
     * @return Will always return {@code null}.
     */
    @Override public R reduce(List<ComputeJobResult> results) {
        return null;
    }
}