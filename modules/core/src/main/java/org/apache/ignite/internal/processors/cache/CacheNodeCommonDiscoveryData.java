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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache information sent in discovery data to joining node.
 */
public class CacheNodeCommonDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final Map<String, CacheData> caches;

    /** */
    @GridToStringInclude
    private final Map<String, CacheData> templates;

    /** */
    @GridToStringInclude
    private final Map<Integer, CacheGroupData> cacheGrps;

    /** */
    private final Map<String, Map<UUID, Boolean>> clientNodesMap;

    /** */
    private Collection<String> restartingCaches;

    /**
     * @param caches Started caches.
     * @param templates Configured templates.
     * @param cacheGrps Started cache groups.
     * @param clientNodesMap Information about cache client nodes.
     */
    public CacheNodeCommonDiscoveryData(Map<String, CacheData> caches,
        Map<String, CacheData> templates,
        Map<Integer, CacheGroupData> cacheGrps,
        Map<String, Map<UUID, Boolean>> clientNodesMap,
        Collection<String> restartingCaches
    ) {
        assert caches != null;
        assert templates != null;
        assert cacheGrps != null;
        assert clientNodesMap != null;

        this.caches = caches;
        this.templates = templates;
        this.cacheGrps = cacheGrps;
        this.clientNodesMap = clientNodesMap;
        this.restartingCaches = restartingCaches;
    }

    /**
     * @return Started cache groups.
     */
    Map<Integer, CacheGroupData> cacheGroups() {
        return cacheGrps;
    }

    /**
     * @return Started caches.
     */
    public Map<String, CacheData> caches() {
        return caches;
    }

    /**
     * @return Configured templates.
     */
    public Map<String, CacheData> templates() {
        return templates;
    }

    /**
     * @return Information about cache client nodes.
     */
    public Map<String, Map<UUID, Boolean>> clientNodesMap() {
        return clientNodesMap;
    }

    /**
     * @return A collection of restarting cache names.
     */
    Collection<String> restartingCaches() {
        return restartingCaches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheNodeCommonDiscoveryData.class, this);
    }
}
