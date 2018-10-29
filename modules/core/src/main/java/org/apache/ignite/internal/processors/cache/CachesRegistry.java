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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Class is responsible to hold and persist cache and cache group descriptors.
 */
public class CachesRegistry {
    /** Logger. */
    private final IgniteLogger log;

    /** Cache shared context. */
    private final GridCacheSharedContext cctx;

    /** Registered cache groups (updated from exchange thread). */
    private final ConcurrentHashMap<Integer, CacheGroupDescriptor> registeredGrps = new ConcurrentHashMap<>();

    /** Registered caches (updated from exchange thread). */
    private final ConcurrentHashMap<Integer, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** Last registered caches configuration persist future. */
    private volatile IgniteInternalFuture<?> cachesConfPersistFuture;

    /**
     * @param cctx Cache shared context.
     */
    public CachesRegistry(GridCacheSharedContext cctx) {
        assert cctx != null;

        this.cctx = cctx;
        this.log = cctx.logger(getClass());
    }

    /**
     * Removes currently registered cache groups and caches.
     * Adds given cache groups and caches to registry.
     *
     * @param groupDescriptors Registered groups.
     * @param cacheDescriptors Registered caches.
     * @return Future that will be completed when all caches configurations will be persisted.
     */
    public IgniteInternalFuture<?> init(
        Map<Integer, CacheGroupDescriptor> groupDescriptors,
        Map<String, DynamicCacheDescriptor> cacheDescriptors
    ) {
        unregisterAll();

        return registerAllCachesAndGroups(groupDescriptors.values(), cacheDescriptors.values());
    }

    /**
     * Adds cache group to registry.
     *
     * @param grpDesc Group description.
     * @return Previously registered cache group or {@code null} otherwise.
     */
    private CacheGroupDescriptor registerGroup(CacheGroupDescriptor grpDesc) {
        return registeredGrps.put(grpDesc.groupId(), grpDesc);
    }

    /**
     * Adds cache to registry.
     *
     * @param desc Cache description.
     * @return Previously registered cache or {@code null} otherwise.
     */
    private DynamicCacheDescriptor registerCache(DynamicCacheDescriptor desc) {
        return registeredCaches.put(desc.cacheId(), desc);
    }

    /**
     * Removes cache group from registry.
     *
     * @param grpId Group id.
     * @return Unregistered cache group or {@code null} if group doesn't exist.
     */
    public CacheGroupDescriptor unregisterGroup(int grpId) {
        return registeredGrps.remove(grpId);
    }

    /**
     * @return All registered cache groups.
     */
    public Map<Integer, CacheGroupDescriptor> allGroups() {
        return Collections.unmodifiableMap(registeredGrps);
    }

    /**
     * @param grpId Group ID.
     * @return Group descriptor.
     */
    public CacheGroupDescriptor group(int grpId) {
        CacheGroupDescriptor desc = registeredGrps.get(grpId);

        assert desc != null : grpId;

        return desc;
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache descriptor if cache found.
     */
    @Nullable public DynamicCacheDescriptor cache(int cacheId) {
        return registeredCaches.get(cacheId);
    }

    /**
     * Removes cache from registry.
     *
     * @param cacheId Cache id.
     * @return Unregistered cache or {@code null} if cache doesn't exist.
     */
    @Nullable public DynamicCacheDescriptor unregisterCache(int cacheId) {
        return registeredCaches.remove(cacheId);
    }

    /**
     * @return All registered cache groups.
     */
    public Map<Integer, DynamicCacheDescriptor> allCaches() {
        return Collections.unmodifiableMap(registeredCaches);
    }

    /**
     * Adds cache and caches groups that is not registered yet to registry.
     *
     * @param descs Cache and cache group descriptors.
     * @return Future that will be completed when all unregistered cache configurations will be persisted.
     */
    public IgniteInternalFuture<?> addUnregistered(Collection<DynamicCacheDescriptor> descs) {
        Collection<CacheGroupDescriptor> groups = descs.stream()
            .map(DynamicCacheDescriptor::groupDescriptor)
            .filter(grpDesc -> !registeredGrps.containsKey(grpDesc.groupId()))
            .collect(Collectors.toList());

        Collection<DynamicCacheDescriptor> caches = descs.stream()
            .filter(cacheDesc -> !registeredCaches.containsKey(cacheDesc.cacheId()))
            .collect(Collectors.toList());

        return registerAllCachesAndGroups(groups, caches);
    }

    /**
     * Adds caches and cache groups to start from {@code exchActions}.
     * Removes caches and caches groups to stop from {@code exchActions}.
     *
     * @param exchActions Exchange actions.
     * @return Future that will be completed when all unregistered cache configurations will be persisted.
     */
    public IgniteInternalFuture<?> update(ExchangeActions exchActions) {
        for (ExchangeActions.CacheGroupActionData stopAction : exchActions.cacheGroupsToStop()) {
            CacheGroupDescriptor rmvd = unregisterGroup(stopAction.descriptor().groupId());

            assert rmvd != null : stopAction.descriptor().cacheOrGroupName();
        }

        for (ExchangeActions.CacheActionData req : exchActions.cacheStopRequests())
            unregisterCache(req.descriptor().cacheId());

        Collection<CacheGroupDescriptor> grpDescs = exchActions.cacheGroupsToStart().stream()
            .map(ExchangeActions.CacheGroupActionData::descriptor)
            .collect(Collectors.toList());

        Collection<DynamicCacheDescriptor> cacheDescs = exchActions.cacheStartRequests().stream()
            .map(ExchangeActions.CacheActionData::descriptor)
            .collect(Collectors.toList());

        return registerAllCachesAndGroups(grpDescs, cacheDescs);
    }

    /**
     *
     */
    public void unregisterAll() {
        registeredGrps.clear();

        registeredCaches.clear();
    }

    /**
     * Awaits last registered caches configurations persist future.
     */
    private void waitLastRegistration() {
        IgniteInternalFuture<?> currentFut = cachesConfPersistFuture;

        if (currentFut != null && !currentFut.isDone()) {
            try {
                currentFut.get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to wait for last registered caches registration future", e);
            }

            if (log.isInfoEnabled())
                log.info("Successfully awaited for last registered caches registration future");
        }
    }

    /**
     * Registers caches and groups.
     * Persists caches configurations on disk if needed.
     *
     * @param groupDescriptors Cache group descriptors.
     * @param cacheDescriptors Cache descriptors.
     * @return Future that will be completed when all unregistered cache configurations will be persisted.
     */
    private IgniteInternalFuture<?> registerAllCachesAndGroups(
        Collection<CacheGroupDescriptor> groupDescriptors,
        Collection<DynamicCacheDescriptor> cacheDescriptors
    ) {
        waitLastRegistration();

        for (CacheGroupDescriptor grpDesc : groupDescriptors)
            registerGroup(grpDesc);

        for (DynamicCacheDescriptor cacheDesc : cacheDescriptors)
            registerCache(cacheDesc);

        List<DynamicCacheDescriptor> cachesToPersist = cacheDescriptors.stream()
            .filter(cacheDesc -> shouldPersist(cacheDesc.cacheConfiguration()))
            .collect(Collectors.toList());

        if (cachesToPersist.isEmpty())
            return cachesConfPersistFuture = new GridFinishedFuture<>();

        return cachesConfPersistFuture = persistCacheConfigurations(cachesToPersist);
    }

    /**
     * Checks whether given cache configuration should be persisted.
     *
     * @param cacheCfg Cache config.
     * @return {@code True} if cache configuration should be persisted, {@code false} in other case.
     */
    private boolean shouldPersist(CacheConfiguration<?, ?> cacheCfg) {
        return cctx.pageStore() != null &&
            CU.isPersistentCache(cacheCfg, cctx.gridConfig().getDataStorageConfiguration()) &&
            !cctx.kernalContext().clientNode();
    }

    /**
     * Persists cache configurations from given {@code cacheDescriptors}.
     *
     * @param cacheDescriptors Cache descriptors to retrieve cache configurations.
     * @return Future that will be completed when all cache configurations will be persisted to cache work directory.
     */
    private IgniteInternalFuture<?> persistCacheConfigurations(List<DynamicCacheDescriptor> cacheDescriptors) {
        List<StoredCacheData> cacheConfigsToPersist = cacheDescriptors.stream()
            .map(cacheDesc -> new StoredCacheData(cacheDesc.cacheConfiguration()).sql(cacheDesc.sql()))
            .collect(Collectors.toList());

        // Pre-create cache work directories if they don't exist.
        for (StoredCacheData data : cacheConfigsToPersist) {
            try {
                cctx.pageStore().checkAndInitCacheWorkDir(data.config());
            }
            catch (IgniteCheckedException e) {
                if (!cctx.kernalContext().isStopping()) {
                    cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                    U.error(log, "Failed to initialize cache work directory for " + data.config(), e);
                }
            }
        }

        return cctx.kernalContext().closure().runLocalSafe(() -> {
            try {
                for (StoredCacheData data : cacheConfigsToPersist)
                    cctx.pageStore().storeCacheData(data, false);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Error while saving cache configurations on disk", e);
            }
        });
    }
}
