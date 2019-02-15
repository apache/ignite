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

package org.apache.ignite.spi.collision.fifoqueue;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean that provides access to the FIFO queue collision SPI configuration.
 */
@MXBeanDescription("MBean provides information about FIFO queue based collision SPI configuration.")
public interface FifoQueueCollisionSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets number of jobs that can be executed in parallel.
     *
     * @return Number of jobs that can be executed in parallel.
     */
    @MXBeanDescription("Number of jobs that can be executed in parallel.")
    public int getParallelJobsNumber();

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param num Parallel jobs number.
     */
    @MXBeanDescription("Number of jobs that can be executed in parallel.")
    public void setParallelJobsNumber(int num);

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @return Maximum allowed number of waiting jobs.
     */
    @MXBeanDescription("Maximum allowed number of waiting jobs.")
    public int getWaitingJobsNumber();

    /**
     * Sets maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @param num Waiting jobs number.
     */
    @MXBeanDescription("Maximum allowed number of waiting jobs.")
    public void setWaitingJobsNumber(int num);

    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    @MXBeanDescription("Number of jobs that wait for execution.")
    public int getCurrentWaitJobsNumber();

    /**
     * Gets current number of jobs that are active, i.e. {@code 'running + held'} jobs.
     *
     * @return Number of active jobs.
     */
    @MXBeanDescription("Number of active jobs.")
    public int getCurrentActiveJobsNumber();

    /**
     * Gets number of currently running (not {@code 'held}) jobs.
     *
     * @return Number of currently running (not {@code 'held}) jobs.
     */
    @MXBeanDescription("Number of running jobs.")
    public int getCurrentRunningJobsNumber();

    /**
     * Gets number of currently {@code 'held'} jobs.
     *
     * @return Number of currently {@code 'held'} jobs.
     */
    @MXBeanDescription("Number of held jobs.")
    public int getCurrentHeldJobsNumber();
}