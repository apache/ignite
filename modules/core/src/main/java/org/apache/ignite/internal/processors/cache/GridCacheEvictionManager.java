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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.mxbean.IgniteMBeanAware;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 *
 */
public class GridCacheEvictionManager extends GridCacheManagerAdapter implements CacheEvictionManager {
    /** Eviction policy. */
    private EvictionPolicy plc;

    /** Eviction filter. */
    private EvictionFilter filter;

    /** Policy enabled. */
    private boolean plcEnabled;

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Stopped flag. */
    private boolean stopped;

    /** First eviction flag. */
    private volatile boolean firstEvictWarn;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        CacheConfiguration cfg = cctx.config();

        if (cctx.isNear()) {
            plc = (cfg.getNearConfiguration().getNearEvictionPolicyFactory() != null) ?
                (EvictionPolicy)cfg.getNearConfiguration().getNearEvictionPolicyFactory().create() :
                cfg.getNearConfiguration().getNearEvictionPolicy();
        }
        else if (cfg.getEvictionPolicyFactory() != null)
            plc = (EvictionPolicy)cfg.getEvictionPolicyFactory().create();
        else
            plc = cfg.getEvictionPolicy();

        plcEnabled = plc != null;

        if (plcEnabled)
            prepare(cfg, plc, cctx.isNear());

        filter = cfg.getEvictionFilter();

        if (log.isDebugEnabled())
            log.debug("Eviction manager started on node: " + cctx.nodeId());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        busyLock.block();

        try {
            if (log.isDebugEnabled())
                log.debug("Eviction manager stopped on node: " + cctx.nodeId());
        }
        finally {
            stopped = true;

            busyLock.unblock();
        }
    }

    /**
     * @return {@code True} if entered busy.
     */
    private boolean enterBusy() {
        if (!busyLock.enterBusy())
            return false;

        if (stopped) {
            busyLock.leaveBusy();

            return false;
        }

        return true;
    }

    /**
     * @param cache Cache from which to evict entry.
     * @param entry Entry to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Filter.
     * @param explicit If eviction is initiated by user.
     * @return {@code true} if entry has been evicted.
     * @throws IgniteCheckedException If failed to evict entry.
     */
    private boolean evict0(
        GridCacheAdapter cache,
        GridCacheEntryEx entry,
        GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter,
        boolean explicit
    ) throws IgniteCheckedException {
        assert cache != null;
        assert entry != null;
        assert obsoleteVer != null;

        boolean recordable = cctx.events().isRecordable(EVT_CACHE_ENTRY_EVICTED);

        CacheObject oldVal = recordable ? entry.rawGet() : null;

        boolean hasVal = recordable && entry.hasValue();

        boolean evicted = entry.evictInternal(obsoleteVer, filter, false);

        if (evicted) {
            // Remove manually evicted entry from policy.
            if (explicit && plcEnabled)
                notifyPolicy(entry);

            cache.removeEntry(entry);

            if (cctx.statisticsEnabled())
                cache.metrics0().onEvict();

            if (recordable)
                cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (IgniteUuid)null, null,
                    EVT_CACHE_ENTRY_EVICTED, null, false, oldVal, hasVal, null, null, null, false);

            if (log.isDebugEnabled())
                log.debug("Entry was evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Entry was not evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }

        return evicted;
    }

    /** {@inheritDoc} */
    @Override public void touch(IgniteTxEntry txEntry, boolean loc) {
        assert txEntry.context() == cctx : "Entry from another cache context passed to eviction manager: [" +
            "entry=" + txEntry +
            ", cctx=" + cctx +
            ", entryCtx=" + txEntry.context() + "]";

        if (!plcEnabled)
            return;

        if (!loc) {
            if (cctx.isNear())
                return;
        }

        GridCacheEntryEx e = txEntry.cached();

        if (e.detached() || e.isInternal())
            return;

        try {
            if (e.markObsoleteIfEmpty(null) || e.obsolete())
                e.context().cache().removeEntry(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + e, ex);
        }

        notifyPolicy(e);
    }

    /** {@inheritDoc} */
    @Override public void touch(GridCacheEntryEx e, AffinityTopologyVersion topVer) {
        assert e.context() == cctx : "Entry from another cache context passed to eviction manager: [" +
            "entry=" + e +
            ", cctx=" + cctx +
            ", entryCtx=" + e.context() + "]";

        if (e.detached() || e.isInternal())
            return;

        try {
            if (e.markObsoleteIfEmpty(null) || e.obsolete())
                e.context().cache().removeEntry(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + e, ex);
        }

        if (!plcEnabled)
            return;

        if (!enterBusy())
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Touching entry [entry=" + e + ", localNode=" + cctx.nodeId() + ']');

            notifyPolicy(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Warns on first eviction.
     */
    private void warnFirstEvict() {
        // Do not move warning output to synchronized block (it causes warning in IDE).
        synchronized (this) {
            if (firstEvictWarn)
                return;

            firstEvictWarn = true;
        }

        U.warn(log, "Evictions started (cache may have reached its capacity)." +
            " You may wish to increase 'maxSize' on eviction policy being used for cache: " + cctx.name());
    }

    /** {@inheritDoc} */
    @Override public boolean evict(@Nullable GridCacheEntryEx entry, @Nullable GridCacheVersion obsoleteVer,
        boolean explicit, @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        assert entry == null || entry.context() == cctx : "Entry from another cache context passed to eviction manager: [" +
            "entry=" + entry +
            ", cctx=" + cctx +
            ", entryCtx=" + entry.context() + "]";

        if (entry == null)
            return true;

        // Do not evict internal entries.
        if (entry.key() instanceof GridCacheInternal)
            return false;

        if (!cctx.isNear() && !explicit && !firstEvictWarn)
            warnFirstEvict();

        if (obsoleteVer == null)
            obsoleteVer = cctx.versions().next();

        // Do not touch entry if not evicted:
        // 1. If it is call from policy, policy tracks it on its own.
        // 2. If it is explicit call, entry is touched on tx commit.
        return evict0(cctx.cache(), entry, obsoleteVer, filter, explicit);
    }

    /** {@inheritDoc} */
    @Override public void batchEvict(Collection<?> keys, @Nullable GridCacheVersion obsoleteVer)
        throws IgniteCheckedException {
        boolean recordable = cctx.events().isRecordable(EVT_CACHE_ENTRY_EVICTED);

        GridCacheAdapter cache = cctx.cache();

        // Get all participating entries to avoid deadlock.
        for (Object k : keys) {
            KeyCacheObject cacheKey = cctx.toCacheKeyObject(k);

            GridCacheEntryEx entry = cache.peekEx(cacheKey);

            if (entry != null && entry.evictInternal(GridCacheVersionManager.EVICT_VER, null, false)) {
                if (plcEnabled)
                    notifyPolicy(entry);

                if (recordable)
                    cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (IgniteUuid)null, null,
                        EVT_CACHE_ENTRY_EVICTED, null, false, entry.rawGet(), entry.hasValue(), null, null, null,
                        false);
            }
        }
    }

    /**
     * @param e Entry to notify eviction policy.
     */
    @SuppressWarnings({"IfMayBeConditional", "RedundantIfStatement"})
    private void notifyPolicy(GridCacheEntryEx e) {
        assert plcEnabled;
        assert plc != null;
        assert !e.isInternal() : "Invalid entry for policy notification: " + e;
        assert e.context() == cctx : "Entry from another cache context passed to eviction manager: [" +
            "entry=" + e +
            ", cctx=" + cctx +
            ", entryCtx=" + e.context() + "]";

        if (log.isDebugEnabled())
            log.debug("Notifying eviction policy with entry: " + e);

        if (filter == null || filter.evictAllowed(e.wrapLazyValue(cctx.keepBinary())))
            plc.onEntryAccessed(e.obsoleteOrDeleted(), e.wrapEviction());
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Eviction manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() +
            ", cache=" + cctx.name() + ']');
    }

    /** For test purposes. */
    public EvictionPolicy getEvictionPolicy() {
        return plc;
    }

    /**
     * Performs injections and MBean registration.
     *
     * @param cfg Cache configuration.
     * @param rsrc Resource.
     * @param near Near flag.
     * @throws IgniteCheckedException If failed.
     */
    private void prepare(CacheConfiguration cfg, @Nullable Object rsrc, boolean near) throws IgniteCheckedException {
        cctx.kernalContext().resource().injectGeneric(rsrc);

        cctx.kernalContext().resource().injectCacheName(rsrc, cfg.getName());

        registerMbean(rsrc, cfg.getName(), near);
    }

    /**
     * Registers MBean for cache components.
     *
     * @param obj Cache component.
     * @param cacheName Cache name.
     * @param near Near flag.
     * @throws IgniteCheckedException If registration failed.
     */
    @SuppressWarnings("unchecked")
    private void registerMbean(Object obj, @Nullable String cacheName, boolean near)
        throws IgniteCheckedException {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        assert obj != null;

        MBeanServer srvr = cctx.kernalContext().config().getMBeanServer();

        assert srvr != null;

        cacheName = U.maskName(cacheName);

        cacheName = near ? cacheName + "-near" : cacheName;

        final Object mbeanImpl = (obj instanceof IgniteMBeanAware) ? ((IgniteMBeanAware)obj).getMBean() : obj;

        for (Class<?> itf : mbeanImpl.getClass().getInterfaces()) {
            if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
                try {
                    U.registerMBean(srvr, cctx.kernalContext().igniteInstanceName(), cacheName, obj.getClass().getName(),
                        mbeanImpl, (Class<Object>)itf);
                }
                catch (Throwable e) {
                    throw new IgniteCheckedException("Failed to register MBean for component: " + obj, e);
                }

                break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel, boolean destroy) {
        cleanup(cctx.config(), plc, cctx.isNear());
    }

    /**
     * @param cfg Cache configuration.
     * @param rsrc Resource.
     * @param near Near flag.
     */
    void cleanup(CacheConfiguration cfg, @Nullable Object rsrc, boolean near) {
        if (rsrc != null) {
            unregisterMbean(rsrc, cfg.getName(), near);

            try {
                cctx.kernalContext().resource().cleanupGeneric(rsrc);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to cleanup resource: " + rsrc, e);
            }
        }
    }

    /**
     * Unregisters MBean for cache components.
     *
     * @param o Cache component.
     * @param cacheName Cache name.
     * @param near Near flag.
     */
    private void unregisterMbean(Object o, @Nullable String cacheName, boolean near) {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        assert o != null;

        MBeanServer srvr = cctx.kernalContext().config().getMBeanServer();

        assert srvr != null;

        cacheName = U.maskName(cacheName);

        cacheName = near ? cacheName + "-near" : cacheName;

        boolean needToUnregister = o instanceof IgniteMBeanAware;

        if (!needToUnregister) {
            for (Class<?> itf : o.getClass().getInterfaces()) {
                if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
                    needToUnregister = true;

                    break;
                }
            }
        }

        if (needToUnregister) {
            try {
                srvr.unregisterMBean(U.makeMBeanName(cctx.kernalContext().igniteInstanceName(), cacheName, o.getClass().getName()));
            }
            catch (Throwable e) {
                U.error(log, "Failed to unregister MBean for component: " + o, e);
            }
        }
    }
}
