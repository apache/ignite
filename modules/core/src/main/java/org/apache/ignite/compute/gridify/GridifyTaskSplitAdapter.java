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

package org.apache.ignite.compute.gridify;

import java.util.List;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;

/**
 * Convenience adapter for tasks that work with {@link Gridify} annotation
 * for grid-enabling methods. It enhances the regular {@link org.apache.ignite.compute.ComputeTaskSplitAdapter}
 * by enforcing the argument type of {@link GridifyArgument}. All tasks
 * that work with {@link Gridify} annotation receive an argument of this type.
 * <p>
 * Please refer to {@link org.apache.ignite.compute.ComputeTaskSplitAdapter} documentation for more information
 * on additional functionality this adapter provides.
 * @param <R> Return value of the task (see {@link org.apache.ignite.compute.ComputeTask#reduce(List)} method).
 */
public abstract class GridifyTaskSplitAdapter<R> extends ComputeTaskSplitAdapter<GridifyArgument, R> {
    /** */
    private static final long serialVersionUID = 0L;

    // No-op.
}