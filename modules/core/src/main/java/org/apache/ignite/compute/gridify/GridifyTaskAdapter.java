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

package org.apache.ignite.compute.gridify;

import java.util.List;
import org.apache.ignite.compute.ComputeTaskAdapter;

/**
 * Convenience adapter for tasks that work with {@link Gridify} annotation
 * for grid-enabling methods. It enhances the regular {@link org.apache.ignite.compute.ComputeTaskAdapter}
 * by enforcing the argument type of {@link GridifyArgument}. All tasks
 * that work with {@link Gridify} annotation receive an argument of this type.
 * <p>
 * Please refer to {@link org.apache.ignite.compute.ComputeTaskAdapter} documentation for more information
 * on additional functionality this adapter provides.
 * @param <R> Return value of the task (see {@link org.apache.ignite.compute.ComputeTask#reduce(List)} method).
 */
public abstract class GridifyTaskAdapter<R> extends ComputeTaskAdapter<GridifyArgument, R> {
    /** */
    private static final long serialVersionUID = 0L;

    // No-op.
}