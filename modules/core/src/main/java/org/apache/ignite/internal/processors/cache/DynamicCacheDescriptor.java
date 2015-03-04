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

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;

/**
 * Cache start descriptor.
 */
public class DynamicCacheDescriptor implements Serializable {
    /** Cache start ID. */
    private IgniteUuid startId;

    /** Cache configuration. */
    @GridToStringExclude
    private CacheConfiguration cacheCfg;

    /** Deploy filter bytes. */
    @GridToStringExclude
    private IgnitePredicate<ClusterNode> nodeFilter;

    /**
     * @param cacheCfg Cache configuration.
     * @param nodeFilter Node filter.
     */
    public DynamicCacheDescriptor(CacheConfiguration cacheCfg, IgnitePredicate<ClusterNode> nodeFilter, IgniteUuid startId) {
        this.cacheCfg = cacheCfg;
        this.nodeFilter = nodeFilter;
        this.startId = startId;
    }

    /**
     * @return Start ID.
     */
    public IgniteUuid startId() {
        return startId;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * @return Node filter.
     */
    public IgnitePredicate<ClusterNode> nodeFilter() {
        return nodeFilter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", cacheCfg.getName());
    }
}
