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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.LinkedList;
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
    private Collection<KeyCacheObject> mappedKeys = new LinkedList<>();

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
    public String toString() {
        return S.toString(GridNearLockMapping.class, this);
    }
}