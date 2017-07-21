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

package org.apache.ignite.spi.collision;

import java.util.Collection;

/**
 * Context for resolving collisions. This context contains collections of
 * waiting jobs, active jobs, and held jobs. If continuations are not used
 * (see {@link org.apache.ignite.compute.ComputeJobContinuation}), then collection of held jobs will
 * always be empty. {@link CollisionSpi} will manipulate these lists
 * to make sure that only allowed number of jobs are running in parallel or
 * waiting to be executed.
 * @since 3.5
 */
public interface CollisionContext {
    /**
     * Gets ordered collection of collision contexts for jobs that are currently executing.
     * It can be empty but never {@code null}.
     *
     * @return Ordered number of collision contexts for currently executing jobs.
     */
    public Collection<CollisionJobContext> activeJobs();

    /**
     * Gets ordered collection of collision contexts for jobs that are currently waiting
     * for execution. It can be empty but never {@code null}. Note that a newly
     * arrived job, if any, will always be represented by the last item in this list.
     * <p>
     * This list is guaranteed not to change while
     * {@link CollisionSpi#onCollision(CollisionContext)} method is being executed.
     *
     * @return Ordered collection of collision contexts for waiting jobs.
     */
    public Collection<CollisionJobContext> waitingJobs();

    /**
     * Gets collection of jobs that are currently in {@code held} state. Job can enter
     * {@code held} state by calling {@link org.apache.ignite.compute.ComputeJobContinuation#holdcc()} method at
     * which point job will release all resources and will get suspended. If
     * {@link org.apache.ignite.compute.ComputeJobContinuation job continuations} are not used, then this list
     * will always be empty, but never {@code null}.
     *
     * @return Collection of jobs that are currently in {@code held} state.
     */
    public Collection<CollisionJobContext> heldJobs();
}