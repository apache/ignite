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

package org.apache.ignite.spi.checkpoint.cache;

import javax.cache.CacheException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.checkpoint.CheckpointListener;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * This class defines cache-based implementation for checkpoint SPI.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 * <li>Cache name (see {@link #setCacheName(String)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * {@link CacheCheckpointSpi} can be configured as follows:
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * String cacheName = "checkpoints";
 *
 * GridCacheConfiguration cacheConfig = new GridCacheConfiguration();
 *
 * cacheConfig.setName(cacheName);
 *
 * GridCacheCheckpointSpi spi = new GridCacheCheckpointSpi();
 *
 * spi.setCacheName(cacheName);
 *
 * cfg.setCacheConfiguration(cacheConfig);
 *
 * // Override default checkpoint SPI.
 * cfg.setCheckpointSpi(cpSpi);
 *
 * // Start grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * {@link CacheCheckpointSpi} can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *     ...
 *         &lt;!-- Cache configuration. --&gt;
 *         &lt;property name=&quot;cacheConfiguration&quot;&gt;
 *             &lt;list&gt;
 *                 &lt;bean class=&quot;org.apache.ignite.cache.CacheConfiguration&quot;&gt;
 *                     &lt;property name=&quot;name&quot; value=&quot;CACHE_NAME&quot;/&gt;
 *                 &lt;/bean&gt;
 *             &lt;/list&gt;
 *         &lt;/property&gt;
 *
 *         &lt;!-- SPI configuration. --&gt;
 *         &lt;property name=&quot;checkpointSpi&quot;&gt;
 *             &lt;bean class=&quot;org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi&quot;&gt;
 *                 &lt;property name=&quot;cacheName&quot; value=&quot;CACHE_NAME&quot;/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.checkpoint.CheckpointSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
public class CacheCheckpointSpi extends IgniteSpiAdapter implements CheckpointSpi, CacheCheckpointSpiMBean {
    /** Default cache name (value is <tt>checkpoints</tt>). */
    public static final String DFLT_CACHE_NAME = "checkpoints";

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache name. */
    private String cacheName = DFLT_CACHE_NAME;

    /** Listener. */
    private CheckpointListener lsnr;

    /** Grid event listener. */
    private GridLocalEventListener evtLsnr;

    /**
     * Sets cache name to be used by this SPI.
     * <p>
     * If cache name is not provided {@link #DFLT_CACHE_NAME} is used.
     *
     * @param cacheName Cache name.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public String getCacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        assertParameter(!F.isEmpty(cacheName), "!F.isEmpty(cacheName)");

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(configInfo("cacheName", cacheName));

        registerMBean(gridName, this, CacheCheckpointSpiMBean.class);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        getSpiContext().addLocalEventListener(evtLsnr = new GridLocalEventListener() {
            /** {@inheritDoc} */
            @Override public void onEvent(Event evt) {
                assert evt != null;
                assert evt.type() == EVT_CACHE_OBJECT_REMOVED || evt.type() == EVT_CACHE_OBJECT_EXPIRED;

                CacheEvent e = (CacheEvent)evt;

                if (!F.eq(e.cacheName(), cacheName))
                    return;

                if (e.oldValue() != null) {
                    CheckpointListener tmp = lsnr;

                    if (tmp != null)
                        tmp.onCheckpointRemoved((String)e.key());
                }
            }
        }, EVT_CACHE_OBJECT_REMOVED, EVT_CACHE_OBJECT_EXPIRED);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        if (evtLsnr != null) {
            IgniteSpiContext ctx = getSpiContext();

            if (ctx != null)
                ctx.removeLocalEventListener(evtLsnr);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
        assert key != null;

        try {
            return getSpiContext().get(cacheName, key);
        }
        catch (CacheException e) {
            throw new IgniteSpiException("Failed to load checkpoint data [key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite)
        throws IgniteSpiException {
        assert key != null;
        assert timeout >= 0;

        try {
            if (overwrite) {
                getSpiContext().put(cacheName, key, state, timeout);

                return true;
            }
            else
                return getSpiContext().putIfAbsent(cacheName, key, state, timeout) == null;
        }
        catch (CacheException e) {
            throw new IgniteSpiException("Failed to save checkpoint data [key=" + key +
                ", stateSize=" + state.length + ", timeout=" + timeout + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        assert key != null;

        try {
            return getSpiContext().remove(cacheName, key) != null;
        }
        catch (CacheException e) {
            U.error(log, "Failed to remove checkpoint data [key=" + key + ']', e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void setCheckpointListener(CheckpointListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheCheckpointSpi.class, this);
    }
}