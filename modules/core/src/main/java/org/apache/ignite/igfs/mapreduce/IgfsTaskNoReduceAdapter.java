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