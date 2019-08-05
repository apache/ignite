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

package org.apache.ignite.internal.processors.metric.list.view;

import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;

/**
 *
 */
public class CacheView implements MonitoringRow<String> {
    /** Cache. */
    private DynamicCacheDescriptor cache;

    /** Idenity of the user activity causes cache creation. */
    private String sessionId;

    /**
     * @param sessionId Idenity of the user activity causes cache creation.
     * @param cache Cache.
     */
    public CacheView(String sessionId, DynamicCacheDescriptor cache) {
        this.cache = cache;
        this.sessionId = sessionId;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return sessionId;
    }

    /** */
    public int groupId() {
        return cache.groupId();
    }

    /** */
    public String groupName() {
        return cache.groupDescriptor().cacheOrGroupName();
    }

    /** */
    public Integer cacheId() {
        return cache.cacheId();
    }

    /** */
    public String cacheName() {
        return cache.cacheName();
    }

    /** */
    public String cacheType() {
        return cache.cacheType().name();
    }

}
