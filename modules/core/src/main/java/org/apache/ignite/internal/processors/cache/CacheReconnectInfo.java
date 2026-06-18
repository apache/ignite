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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class CacheReconnectInfo implements Message {
    /** */
    @Order(0)
    String cacheName;

    /** */
    @Order(1)
    IgniteUuid deploymentId;

    /** */
    @Order(2)
    boolean nearCache;

    /** */
    public CacheReconnectInfo() { }

    /**
     * @param cacheName    Cache name.
     * @param deploymentId Cache deployment ID.
     * @param nearCache    Near cache flag.
     */
    public CacheReconnectInfo(String cacheName, IgniteUuid deploymentId, boolean nearCache) {
        assert cacheName != null;
        assert deploymentId != null;

        this.cacheName = cacheName;
        this.deploymentId = deploymentId;
        this.nearCache = nearCache;
    }

    /**
     * @return Cache configuration.
     */
    String cacheName() {
        return cacheName;
    }

    /**
     * @return Cache deployment ID.
     */
    IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @return Near cache flag.
     */
    boolean nearCache() {
        return nearCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return S.toString(CacheReconnectInfo.class, this);
    }
}
