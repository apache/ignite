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

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskSession;

/**
 * This interface defines set of operations that collision SPI implementation can perform on
 * jobs that are either waiting or executing.
 */
public interface CollisionJobContext {
    /**
     * Gets current task session associated with this job.
     *
     * @return Grid task session.
     */
    public ComputeTaskSession getTaskSession();

    /**
     * Gets job context. Use this context to set/get attributes that
     * should be visible only to this job and should not be distributed
     * to other jobs in the grid.
     * <p>
     * Job context travels with job whenever it gets failed-over to another
     * node, so attributes set on the context on one node will be visible
     * on other nodes this job may potentially end up on.
     *
     * @return Job context.
     */
    public ComputeJobContext getJobContext();

    /**
     * Job for this context.
     *
     * @return Job for this context.
     */
    public ComputeJob getJob();

    /**
     * Activates the job. If job is already active this is no-op. Collision resolution
     * is handled concurrently, so it may be possible that other threads already activated
     * or cancelled/rejected this job. This method will return {@code true} if it was
     * able to activate the job, and {@code false} otherwise.
     *
     * @return {@code True} if it was possible to activate the job, and
     *      {@code false} otherwise.
     */
    public boolean activate();

    /**
     * Cancels the job. If job was active (executing) method {@link org.apache.ignite.compute.ComputeJob#cancel()} will
     * be called on the job. If job was in wait state, then it will be {@code rejected}
     * prior to execution and {@link org.apache.ignite.compute.ComputeJob#cancel()} will not be called.
     * <p>
     * Collision resolution is handled concurrently, so it may be possible that other threads
     * already activated or cancelled/rejected this job. This method will return {@code true}
     * if it was able to cancel/reject the job and {@code false} otherwise.
     *
     * @return {@code True} if it was possible to cancel/reject this job, {@code false}
     *      otherwise.
     */
    public boolean cancel();
}