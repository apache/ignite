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

import java.util.Map;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Tests that {@link IgniteCache#invoke(Object, EntryProcessor, Object...)}, {@link IgniteCache#invokeAll(Map, Object...)}
 * overloaded and async methods works fine with near cache when cluster in a {@link ClusterState#ACTIVE_READ_ONLY} mode.
 */
public class IgniteNearCacheInvokeClusterReadOnlyModeSelfTest extends IgniteCacheInvokeClusterReadOnlyModeSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?, ?>[] cacheConfigurations() {
        return filterAndAddNearCacheConfig(super.cacheConfigurations());
    }
}
