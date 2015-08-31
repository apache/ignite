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

import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Collision SPI allows to regulate how grid jobs get executed when they arrive on a
 * destination node for execution. Its functionality is similar to tasks management via
 * <b>customizable</b> GCD (Great Central Dispatch) on Mac OS X as it allows developer to provide
 * custom job dispatching on a single node. In general a grid node will have multiple jobs
 * arriving to it for execution and potentially multiple jobs that are already executing or waiting
 * for execution on it. There are multiple possible strategies dealing with this situation, like
 * all jobs can proceed in parallel, or jobs can be serialized i.e., or only one job can execute
 * in any given point of time, or only certain number or types of grid jobs can proceed in
 * parallel, etc...
 * <p>
 * Collision SPI provides developer with ability to use the custom logic in determining how
 * grid jobs should be executed on a destination grid node. Ignite comes with the following
 * ready implementations for collision resolution that cover most popular strategies:
 * <ul>
 *      <li>{@link org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi}</li>
 *      <li>{@link org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi}</li>
 *      <li>{@link org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface CollisionSpi extends IgniteSpi {
    /**
     * This is a callback called when either new grid job arrived or executing job finished its
     * execution. When new job arrives it is added to the end of the wait list and this
     * method is called. When job finished its execution, it is removed from the active list and
     * this method is called (i.e., when grid job is finished it will not appear in any list
     * in collision resolution).
     * <p>
     * Implementation of this method should act on all lists, each of which contains collision
     * job contexts that define a set of operations available during collision resolution. Refer
     * to {@link CollisionContext} and {@link CollisionJobContext} documentation for
     * more information.
     *
     * @param ctx Collision context which contains all collision lists.
     */
    public void onCollision(CollisionContext ctx);

    /**
     * Listener to be set for notification of external collision events (e.g. job stealing).
     * Once grid receives such notification, it will immediately invoke collision SPI.
     * <p>
     * Ignite uses this listener to enable job stealing from overloaded to underloaded nodes.
     * However, you can also utilize it, for instance, to provide time based collision
     * resolution. To achieve this, you most likely would mark some job by setting a certain
     * attribute in job context (see {@link org.apache.ignite.compute.ComputeJobContext}) for a job that requires
     * time-based scheduling and set some timer in your SPI implementation that would wake up
     * after a certain period of time. Once this period is reached, you would notify this
     * listener that a collision resolution should take place. Then inside of your collision
     * resolution logic, you would find the marked waiting job and activate it.
     * <p>
     * Note that most collision SPI's might not have external collisions. In that case,
     * they should simply ignore this method and do nothing when listener is set.
     *
     * @param lsnr Listener for external collision events.
     */
    public void setExternalCollisionListener(@Nullable CollisionExternalListener lsnr);
}