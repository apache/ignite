/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.compute;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

/**
 * This annotation disables caching of task results when attached to {@link ComputeTask} class
 * being executed. Use it when number of jobs within task grows too big, or jobs themselves
 * are too large to keep in memory throughout task execution. By default all results are cached and passed into
 * {@link ComputeTask#result(ComputeJobResult,List) ComputeTask.result(ComputeJobResult, List&lt;ComputeJobResult&gt;)}
 * method or {@link ComputeTask#reduce(List) ComputeTask.reduce(List&lt;ComputeJobResult&gt;)} method.
 * When this annotation is attached to a task class, then this list of job results will always be empty.
 * <p>
 * Note that if this annotation is attached to a task class, then job siblings list is not maintained
 * and always has size of {@code 0}. This is done to make sure that in case if task emits large
 * number of jobs, list of jobs siblings does not grow. This only affects the following methods
 * on {@link ComputeTaskSession}:
 * <ul>
 * <li>{@link ComputeTaskSession#getJobSiblings()}</li>
 * <li>{@link ComputeTaskSession#getJobSibling(org.apache.ignite.lang.IgniteUuid)}</li>
 * <li>{@link ComputeTaskSession#refreshJobSiblings()}</li>
 * </ul>
 *
 * Use this annotation when job results are too large to hold in memory and can be discarded
 * after being processed in
 * {@link ComputeTask#result(ComputeJobResult, List) ComputeTask.result(ComputeJobResult, List&lt;ComputeJobResult&gt;)}
 * method.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskNoResultCache {
    // No-op.
}