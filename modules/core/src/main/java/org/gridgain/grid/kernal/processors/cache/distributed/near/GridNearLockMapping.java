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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Key-to node mapping.
 */
public class GridNearLockMapping<K, V> {
    /** Node to which keys are mapped. */
    private ClusterNode node;

    /** Collection of mapped keys. */
    @GridToStringInclude
    private Collection<K> mappedKeys = new LinkedList<>();

    /** Near lock request. */
    @GridToStringExclude
    private GridNearLockRequest<K, V> req;

    /** Distributed keys. Key will not be distributed if lock is reentry. */
    @GridToStringInclude
    private Collection<K> distributedKeys;

    /**
     * Creates near lock mapping for specified node and key.
     *
     * @param node Node.
     * @param firstKey First key in mapped keys collection.
     */
    public GridNearLockMapping(ClusterNode node, K firstKey) {
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
    public Collection<K> mappedKeys() {
        return mappedKeys;
    }

    /**
     * @param key Key to add to mapping.
     */
    public void addKey(K key) {
        mappedKeys.add(key);
    }

    /**
     * @return Near lock request.
     */
    @Nullable public GridNearLockRequest<K, V> request() {
        return req;
    }

    /**
     * @param req Near lock request.
     */
    public void request(GridNearLockRequest<K, V> req) {
        assert req != null;

        this.req = req;
    }

    /**
     * @return Collection of distributed keys.
     */
    public Collection<K> distributedKeys() {
        return distributedKeys;
    }

    /**
     * @param distributedKeys Collection of distributed keys.
     */
    public void distributedKeys(Collection<K> distributedKeys) {
        this.distributedKeys = distributedKeys;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridNearLockMapping.class, this);
    }
}
