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

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 * In-memory {@link org.apache.ignite.spi.eventstorage.EventStorageSpi} implementation. All events are
 * kept in the FIFO queue. If no configuration is provided a default expiration
 * {@link #DFLT_EXPIRE_AGE_MS} and default count {@link #DFLT_EXPIRE_COUNT} will
 * be used.
 * <p>
 * It's recommended not to set huge size and unlimited TTL because this might
 * lead to consuming a lot of memory and result in {@link OutOfMemoryError}.
 * Both event expiration time and maximum queue size could be changed at
 * runtime.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>Event queue size (see {@link #setExpireCount(long)})</li>
 * <li>Event time-to-live value (see {@link #setExpireAgeMs(long)})</li>
 * <li>{@link #setFilter(org.apache.ignite.lang.IgnitePredicate)} - Event filter that should be used for decision to accept event.</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * MemoryEventStorageSpi is used by default and should be explicitly configured only
 * if some SPI configuration parameters need to be overridden. Examples below insert own
 * events queue size value that differs from default 10000.
 * <pre name="code" class="java">
 * MemoryEventStorageSpi = new MemoryEventStorageSpi();
 *
 * // Init own events size.
 * spi.setExpireCount(2000);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default event storage SPI.
 * cfg.setEventStorageSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * MemoryEventStorageSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="discoverySpi"&gt;
 *             &lt;bean class="org.apache.ignite.spi.eventStorage.memory.MemoryEventStorageSpi"&gt;
 *                 &lt;property name="expireCount" value="2000"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.eventstorage.EventStorageSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
public class MemoryEventStorageSpi extends IgniteSpiAdapter implements EventStorageSpi,
    MemoryEventStorageSpiMBean {
    /** Default event time to live value in milliseconds (value is {@link Long#MAX_VALUE}). */
    public static final long DFLT_EXPIRE_AGE_MS = Long.MAX_VALUE;

    /** Default expire count (value is {@code 10000}). */
    public static final int DFLT_EXPIRE_COUNT = 10000;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** Event time-to-live value in milliseconds. */
    private long expireAgeMs = DFLT_EXPIRE_AGE_MS;

    /** Maximum queue size. */
    private long expireCnt = DFLT_EXPIRE_COUNT;

    /** Events queue. */
    private ConcurrentLinkedDeque8<Event> evts = new ConcurrentLinkedDeque8<>();

    /** Configured event predicate filter. */
    private IgnitePredicate<Event> filter;

    /**
     * Gets filter for events to be recorded.
     *
     * @return Filter to use.
     */
    public IgnitePredicate<Event> getFilter() {
        return filter;
    }

    /**
     * Sets filter for events to be recorded.
     *
     * @param filter Filter to use.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setFilter(IgnitePredicate<Event> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(expireCnt > 0, "expireCnt > 0");
        assertParameter(expireAgeMs > 0, "expireAgeMs > 0");

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("expireAgeMs", expireAgeMs));
            log.debug(configInfo("expireCnt", expireCnt));
        }

        registerMBean(gridName, this, MemoryEventStorageSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Reset events.
        evts.clear();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Sets events expiration time. All events that exceed this value
     * will be removed from the queue when next event comes.
     * <p>
     * If not provided, default value is {@link #DFLT_EXPIRE_AGE_MS}.
     *
     * @param expireAgeMs Expiration time in milliseconds.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setExpireAgeMs(long expireAgeMs) {
        this.expireAgeMs = expireAgeMs;
    }

    /**
     * Sets events queue size. Events will be filtered out when new request comes.
     * <p>
     * If not provided, default value {@link #DFLT_EXPIRE_COUNT} will be used.
     *
     * @param expireCnt Maximum queue size.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setExpireCount(long expireCnt) {
        this.expireCnt = expireCnt;
    }

    /** {@inheritDoc} */
    @Override public long getExpireAgeMs() {
        return expireAgeMs;
    }

    /** {@inheritDoc} */
    @Override public long getExpireCount() {
        return expireCnt;
    }

    /** {@inheritDoc} */
    @Override public long getQueueSize() {
        return evts.sizex();
    }

    /** {@inheritDoc} */
    @Override public void clearAll() {
        evts.clear();
    }

    /** {@inheritDoc} */
    @Override public <T extends Event> Collection<T> localEvents(IgnitePredicate<T> p) {
        A.notNull(p, "p");

        cleanupQueue();

        return F.retain((Collection<T>)evts, true, p);
    }

    /** {@inheritDoc} */
    @Override public void record(Event evt) throws IgniteSpiException {
        assert evt != null;

        // Filter out events.
        if (filter == null || filter.apply(evt)) {
            cleanupQueue();

            evts.add(evt);

            // Make sure to filter out metrics updates to prevent log from flooding.
            if (evt.type() != EVT_NODE_METRICS_UPDATED && log.isDebugEnabled())
                log.debug("Event recorded: " + evt);
        }
    }

    /**
     * Method cleans up all events that either outnumber queue size
     * or exceeds time-to-live value. It does none if someone else
     * cleans up queue (lock is locked) or if there are queue readers
     * (readersNum > 0).
     */
    private void cleanupQueue() {
        long now = U.currentTimeMillis();

        long queueOversize = evts.sizex() - expireCnt;

        for (int i = 0; i < queueOversize && evts.sizex() > expireCnt; i++) {
            Event expired = evts.poll();

            if (log.isDebugEnabled())
                log.debug("Event expired by count: " + expired);
        }

        while (true) {
            ConcurrentLinkedDeque8.Node<Event> node = evts.peekx();

            if (node == null) // Queue is empty.
                break;

            Event evt = node.item();

            if (evt == null) // Competing with another thread.
                continue;

            if (now - evt.timestamp() < expireAgeMs)
                break;

            if (evts.unlinkx(node) && log.isDebugEnabled())
                log.debug("Event expired by age: " + node.item());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MemoryEventStorageSpi.class, this);
    }
}