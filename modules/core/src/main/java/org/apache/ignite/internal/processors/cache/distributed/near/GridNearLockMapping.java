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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Key-to node mapping.
 */
public class GridNearLockMapping {
    /** Node to which keys are mapped. */
    private ClusterNode node;

    /** Collection of mapped keys. */
    @GridToStringInclude
    private final Collection<KeyCacheObject> mappedKeys = new ArrayList<>();

    /** Near lock request. */
    @GridToStringExclude
    private GridNearLockRequest req;

    /** Distributed keys. Key will not be distributed if lock is reentry. */
    @GridToStringInclude
    private Collection<KeyCacheObject> distributedKeys;

    /**
     * Creates near lock mapping for specified node and key.
     *
     * @param node Node.
     * @param firstKey First key in mapped keys collection.
     */
    public GridNearLockMapping(ClusterNode node, KeyCacheObject firstKey) {
        assert node != null;
        assert firstKey != null;

        this.node = node;

        mappedKeys.add(firstKey);
    }

    /**
     * @return Node to which keys are mapped.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * @return Mapped keys.
     */
    public Collection<KeyCacheObject> mappedKeys() {
        return mappedKeys;
    }

    /**
     * @param key Key to add to mapping.
     */
    public void addKey(KeyCacheObject key) {
        mappedKeys.add(key);
    }

    /**
     * @return Near lock request.
     */
    @Nullable public GridNearLockRequest request() {
        return req;
    }

    /**
     * @param req Near lock request.
     */
    public void request(GridNearLockRequest req) {
        assert req != null;

        this.req = req;
    }

    /**
     * @return Collection of distributed keys.
     */
    public Collection<KeyCacheObject> distributedKeys() {
        return distributedKeys;
    }

    /**
     * @param distributedKeys Collection of distributed keys.
     */
    public void distributedKeys(Collection<KeyCacheObject> distributedKeys) {
        this.distributedKeys = distributedKeys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearLockMapping.class, this);
    }
}
