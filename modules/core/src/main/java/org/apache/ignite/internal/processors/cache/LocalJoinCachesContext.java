/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

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
    private List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches;

    /**
     *
     */
    @GridToStringInclude
    private List<DynamicCacheDescriptor> locJoinInitCaches;

    /** */
    @GridToStringInclude
    private Map<Integer, CacheGroupDescriptor> cacheGrpDescs;

    /** */
    @GridToStringInclude
    private Map<String, DynamicCacheDescriptor> cacheDescs;

    /**
     * @param locJoinStartCaches Local caches to start on join.
     * @param locJoinInitCaches Local caches to initialize query infrastructure without start of caches.
     * @param cacheGrpDescs Cache group descriptors captured during join.
     * @param cacheDescs Cache descriptors captured during join.
     */
    public LocalJoinCachesContext(
        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches,
        List<DynamicCacheDescriptor> locJoinInitCaches,
        Map<Integer, CacheGroupDescriptor> cacheGrpDescs,
        Map<String, DynamicCacheDescriptor> cacheDescs
    ) {
        this.locJoinStartCaches = locJoinStartCaches;
        this.locJoinInitCaches = locJoinInitCaches;
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
     * @return Cache descriptors to initialize query infrastructure without start of caches.
     */
    public List<DynamicCacheDescriptor> initCaches() {
        return locJoinInitCaches;
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

        Iterator<DynamicCacheDescriptor> iter = locJoinInitCaches.iterator();

        for (; iter.hasNext(); ) {
            DynamicCacheDescriptor desc = iter.next();

            if (cacheNames.contains(desc.cacheName()))
                iter.remove();
        }
    }

    /**
     * @return {@code True} if the context is empty.
     */
    public boolean isEmpty() {
        return F.isEmpty(locJoinStartCaches) && F.isEmpty(locJoinInitCaches) && F.isEmpty(cacheGrpDescs) && F.isEmpty(cacheDescs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LocalJoinCachesContext.class, this);
    }
}
