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

package org.apache.ignite.mxbean;

/**
 * MBean that provides access to information about striped executor service.
 */
@MXBeanDescription("MBean that provides access to information about striped executor service.")
public interface StripedExecutorMXBean {
    /**
     * Checks for starvation in striped pool, dumps in log information if potential starvation
     * was found.
     */
    @MXBeanDescription("Checks for starvation in striped pool.")
    public void checkStarvation();

    /**
     * @return Stripes count.
     */
    @MXBeanDescription("Stripes count.")
    public int getStripesCount();

    /**
     *
     * @return {@code True} if this executor has been shut down.
     */
    @MXBeanDescription("True if this executor has been shut down.")
    public boolean isShutdown();

    /**
     * Note that
     * {@code isTerminated()} is never {@code true} unless either {@code shutdown()} or
     * {@code shutdownNow()} was called first.
     *
     * @return {@code True} if all tasks have completed following shut down.
     */
    @MXBeanDescription("True if all tasks have completed following shut down.")
    public boolean isTerminated();

    /**
     * @return Return total queue size of all stripes.
     */
    @MXBeanDescription("Total queue size of all stripes.")
    public int getTotalQueueSize();

    /**
     * @return Completed tasks count.
     */
    @MXBeanDescription("Completed tasks count of all stripes.")
    public long getTotalCompletedTasksCount();

    /**
     * @return Number of completed tasks per stripe.
     */
    @MXBeanDescription("Number of completed tasks per stripe.")
    public long[] getStripesCompletedTasksCounts();

    /**
     * @return Number of active tasks.
     */
    @MXBeanDescription("Number of active tasks of all stripes.")
    public int getActiveCount();

    /**
     * @return Number of active tasks per stripe.
     */
    @MXBeanDescription("Number of active tasks per stripe.")
    public boolean[] getStripesActiveStatuses();

    /**
     * @return Size of queue per stripe.
     */
    @MXBeanDescription("Size of queue per stripe.")
    public int[] getStripesQueueSizes();
}
