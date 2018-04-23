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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Context to capture caches state for a node joining to an active cluster. Since registered caches is updated in
 * discovery thread and caches info are updated in exchange thread, we must capture the state in discovery thread
 * somehow and pass it to the exchange. This class holds the required context.
 */
public class LocalJoinCachesContext {
    /** */
    @GridToStringInclude
    private List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches = Collections.emptyList();

    /** */
    @GridToStringInclude
    private Map<Integer, CacheGroupDescriptor> cacheGrpDescs;

    /** */
    @GridToStringInclude
    private Map<String, DynamicCacheDescriptor> cacheDescs;

    /**
     * @param locJoinStartCaches Local caches to start on join.
     * @param cacheGrpDescs Cache group descriptors captured during join.
     * @param cacheDescs Cache descriptors captured during join.
     */
    public LocalJoinCachesContext(
        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches,
        Map<Integer, CacheGroupDescriptor> cacheGrpDescs,
        Map<String, DynamicCacheDescriptor> cacheDescs
    ) {
        this.locJoinStartCaches = locJoinStartCaches;
        this.cacheGrpDescs = cacheGrpDescs;
        this.cacheDescs = cacheDescs;
    }

    /**
     * @return Caches to start.
     */
    public List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> caches() {
        return locJoinStartCaches;
    }

    /**
     * @return Group descriptors.
     */
    public Map<Integer, CacheGroupDescriptor> cacheGroupDescriptors() {
        return cacheGrpDescs;
    }

    /**
     * @return Cache descriptors.
     */
    public Map<String, DynamicCacheDescriptor> cacheDescriptors() {
        return cacheDescs;
    }

    /**
     * @param cacheNames Survived caches to clean.
     */
    public void removeSurvivedCaches(Set<String> cacheNames) {
        Iterator<T2<DynamicCacheDescriptor, NearCacheConfiguration>> it = locJoinStartCaches.iterator();

        for (; it.hasNext();) {
            T2<DynamicCacheDescriptor, NearCacheConfiguration> entry = it.next();

            DynamicCacheDescriptor desc = entry.get1();

            if (cacheNames.contains(desc.cacheName()))
                it.remove();
        }
    }

    /**
     * @return {@code True} if the context is empty.
     */
    public boolean isEmpty() {
        return F.isEmpty(locJoinStartCaches) && F.isEmpty(cacheGrpDescs) && F.isEmpty(cacheDescs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalJoinCachesContext.class, this);
    }
}
