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
 * {@link ComputeTask#result(ComputeJobResult,List) GridComputeTask.result(GridComputeJobResult, List&lt;GridComputeJobResult&gt;)}
 * method or {@link ComputeTask#reduce(List) GridComputeTask.reduce(List&lt;GridComputeJobResult&gt;)} method.
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
 * {@link ComputeTask#result(ComputeJobResult, List) GridComputeTask.result(GridComputeJobResult, List&lt;GridComputeJobResult&gt;)}
 * method.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ComputeTaskNoResultCache {
    // No-op.
}