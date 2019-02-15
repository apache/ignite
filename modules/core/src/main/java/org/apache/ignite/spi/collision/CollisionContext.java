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