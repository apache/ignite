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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheRegistry {

    /** Dynamic caches. */
    private final ConcurrentMap<Integer, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGrps = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** Caches currently being restarted. */
    private final Collection<String> restartingCaches = new GridConcurrentHashSet<>();

    public Collection<DynamicCacheDescriptor> registeredCachesDesc() {
        return registeredCaches.values();
    }

    public ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches() {
        return registeredCaches;
    }

    public boolean hasRegisteredCache(String name) {
        return registeredCaches.containsKey(name);
    }
    public boolean hasRegisteredTemplate(String name) {
        return registeredTemplates.containsKey(name);
    }

    public Collection<DynamicCacheDescriptor> registeredTemplatesDesc() {
        return registeredTemplates.values();
    }

    public ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates() {
        return registeredTemplates;
    }

    public Collection<CacheGroupDescriptor> registeredGroupsDesc() {
        return registeredCacheGrps.values();
    }

    public ConcurrentMap<Integer, CacheGroupDescriptor> registeredGroups() {
        return registeredCacheGrps;
    }

    public Collection<String> restartingCaches() {
        return restartingCaches;
    }

    public DynamicCacheDescriptor registeredCache(Integer cacheId) {
        return registeredCaches.get(cacheId);
    }

    public DynamicCacheDescriptor registeredCache(String name) {
        int cacheId = CU.cacheId(name);

        DynamicCacheDescriptor descriptor = registeredCaches.get(cacheId);

        if (descriptor == null)
            return null;

        assert Objects.equals(descriptor.cacheName(), name) : "Desc=" + descriptor.cacheName() + ", cache=" + name;

        return descriptor;
    }

    public DynamicCacheDescriptor registeredTemplate(String name) {
        return registeredTemplates.get(name);
    }

    public CacheGroupDescriptor registeredGroup(Integer grpId) {
        return registeredCacheGrps.get(grpId);
    }

    public void registerCache(String name, DynamicCacheDescriptor cacheDescriptor) {
        DynamicCacheDescriptor old = registeredCaches.put(name, cacheDescriptor);

        restartingCaches.remove(name);

        assert old == null;
    }

    public void registerTemplate(String name, DynamicCacheDescriptor cacheDescriptor) {
        DynamicCacheDescriptor old = registeredTemplates.put(name, cacheDescriptor);

    }

    public void registerGroup(Integer grpId, CacheGroupDescriptor descriptor) {
        CacheGroupDescriptor old = registeredCacheGrps.put(grpId, descriptor);

        assert old == null : old;
    }

    public void unregisterCache(String name, DynamicCacheDescriptor cacheDescriptor) {
        boolean remove = registeredCaches.remove(name, cacheDescriptor);

        assert remove : "Dynamic cache map was concurrently modified [cache=" + cacheDescriptor + ']';

        CacheGroupDescriptor grpDesc = registeredCacheGrps.get(cacheDescriptor.groupId());

        assert grpDesc != null && grpDesc.groupId() == cacheDescriptor.groupId() : cacheDescriptor;

        grpDesc.onCacheStopped(name, cacheDescriptor.cacheId());

        if (!grpDesc.hasCaches()) {
            registeredCacheGrps.remove(grpDesc.groupId());
        }
    }

    public void restartCache(String name, DynamicCacheDescriptor cacheDescriptor) {
        unregisterCache(name, cacheDescriptor);

        restartingCaches.add(name);
    }

    public void cleanCachesAndGroups() {
        registeredCaches.clear();
        registeredCacheGrps.clear();
    }

    public void unregisterAll() {
        registeredCacheGrps.clear();
        registeredCaches.clear();
        registeredTemplates.clear();
    }

}
