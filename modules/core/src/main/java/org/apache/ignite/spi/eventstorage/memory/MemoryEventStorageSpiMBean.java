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

package org.apache.ignite.spi.eventstorage.memory;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean for {@link MemoryEventStorageSpi}.
 * Beside properties defined for every SPI bean this one gives access to:
 * <ul>
 * <li>Event expiration time (see {@link #getExpireAgeMs()})</li>
 * <li>Maximum queue size (see {@link #getExpireCount()})</li>
 * <li>Method that removes all items from queue (see {@link #clearAll()})</li>
 * </ul>
 */
@MXBeanDescription("MBean that provides access to memory event storage SPI configuration.")
public interface MemoryEventStorageSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets event time-to-live value. Implementation must guarantee
     * that event would not be accessible if its lifetime exceeds this value.
     *
     * @return Event time-to-live.
     */
    @MXBeanDescription("Event time-to-live value.")
    public long getExpireAgeMs();

    /**
     * Gets maximum event queue size. New incoming events will oust
     * oldest ones if queue size exceeds this limit.
     *
     * @return Maximum event queue size.
     */
    @MXBeanDescription("Maximum event queue size.")
    public long getExpireCount();

    /**
     * Gets current queue size of the event queue.
     *
     * @return Current queue size of the event queue.
     */
    @MXBeanDescription("Current event queue size.")
    public long getQueueSize();

    /**
     * Removes all events from the event queue.
     */
    @MXBeanDescription("Removes all events from the event queue.")
    public void clearAll();
}