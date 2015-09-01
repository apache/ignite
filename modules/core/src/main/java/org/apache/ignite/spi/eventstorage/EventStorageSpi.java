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

package org.apache.ignite.spi.eventstorage;

import java.util.Collection;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * This SPI provides local node events storage. SPI allows for recording local
 * node events and querying recorded local events. Every node during its life-cycle
 * goes through a serious of events such as task deployment, task execution, job
 * execution, etc. For
 * performance reasons Ignite is designed to store all locally produced events
 * locally. These events can be later retrieved using either distributed query:
 * <ul>
 *      <li>{@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)}</li>
 * </ul>
 * or local only query:
 * <ul>
 *      <li>{@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 * @see org.apache.ignite.events.Event
 */
public interface EventStorageSpi extends IgniteSpi {
    /**
     * Queries locally-stored events only. Events could be filtered out
     * by given predicate filter.
     *
     * @param p Event predicate filter.
     * @return Collection of events.
     */
    public <T extends Event> Collection<T> localEvents(IgnitePredicate<T> p);

    /**
     * Records single event.
     *
     * @param evt Event that should be recorded.
     * @throws org.apache.ignite.spi.IgniteSpiException If event recording failed for any reason.
     */
    public void record(Event evt) throws IgniteSpiException;
}