/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.eventstorage.memory;

import org.apache.ignite.lang.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.eventstorage.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * In-memory {@link GridEventStorageSpi} implementation. All events are
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
 * GridMemoryEventStorageSpi is used by default and should be explicitly configured only
 * if some SPI configuration parameters need to be overridden. Examples below insert own
 * events queue size value that differs from default 10000.
 * <pre name="code" class="java">
 * GridMemoryEventStorageSpi = new GridMemoryEventStorageSpi();
 *
 * // Init own events size.
 * spi.setExpireCount(2000);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default event storage SPI.
 * cfg.setEventStorageSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridMemoryEventStorageSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="discoverySpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.eventStorage.memory.GridMemoryEventStorageSpi"&gt;
 *                 &lt;property name="expireCount" value="2000"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridEventStorageSpi
 */
@GridSpiMultipleInstancesSupport(true)
public class GridMemoryEventStorageSpi extends GridSpiAdapter implements GridEventStorageSpi,
    GridMemoryEventStorageSpiMBean {
    /** Default event time to live value in milliseconds (value is {@link Long#MAX_VALUE}). */
    public static final long DFLT_EXPIRE_AGE_MS = Long.MAX_VALUE;

    /** Default expire count (value is {@code 10000}). */
    public static final int DFLT_EXPIRE_COUNT = 10000;

    /** */
    @GridLoggerResource
    private GridLogger log;

    /** Event time-to-live value in milliseconds. */
    private long expireAgeMs = DFLT_EXPIRE_AGE_MS;

    /** Maximum queue size. */
    private long expireCnt = DFLT_EXPIRE_COUNT;

    /** Events queue. */
    private ConcurrentLinkedDeque8<GridEvent> evts = new ConcurrentLinkedDeque8<>();

    /** Configured event predicate filter. */
    private IgnitePredicate<GridEvent> filter;

    /**
     * Gets filter for events to be recorded.
     *
     * @return Filter to use.
     */
    public IgnitePredicate<GridEvent> getFilter() {
        return filter;
    }

    /**
     * Sets filter for events to be recorded.
     *
     * @param filter Filter to use.
     */
    @GridSpiConfiguration(optional = true)
    public void setFilter(IgnitePredicate<GridEvent> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(expireCnt > 0, "expireCnt > 0");
        assertParameter(expireAgeMs > 0, "expireAgeMs > 0");

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("expireAgeMs", expireAgeMs));
            log.debug(configInfo("expireCnt", expireCnt));
        }

        registerMBean(gridName, this, GridMemoryEventStorageSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
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
    @GridSpiConfiguration(optional = true)
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
    @GridSpiConfiguration(optional = true)
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
    @Override public <T extends GridEvent> Collection<T> localEvents(IgnitePredicate<T> p) {
        A.notNull(p, "p");

        cleanupQueue();

        return F.retain((Collection<T>)evts, true, p);
    }

    /** {@inheritDoc} */
    @Override public void record(GridEvent evt) throws GridSpiException {
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
            GridEvent expired = evts.poll();

            if (log.isDebugEnabled())
                log.debug("Event expired by count: " + expired);
        }

        while (true) {
            ConcurrentLinkedDeque8.Node<GridEvent> node = evts.peekx();

            if (node == null) // Queue is empty.
                break;

            GridEvent evt = node.item();

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
        return S.toString(GridMemoryEventStorageSpi.class, this);
    }
}
