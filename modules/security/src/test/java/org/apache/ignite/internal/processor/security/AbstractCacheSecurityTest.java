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

package org.apache.ignite.internal.processor.security;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 *
 */
public abstract class AbstractCacheSecurityTest extends AbstractResolveSecurityContextTest {
    /** Cache name for tests. */
    protected static final String CACHE_READ_ONLY_PERM = "CACHE_READ_ONLY_PERM";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<>()
                .setName(CACHE_READ_ONLY_PERM)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
        };
    }

    /**
     * Getting the key that is contained on primary partition on passed node for given cache.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected Integer primaryKey(IgniteEx ignite) {
        Affinity<Integer> affinity = ignite.affinity(CACHE_READ_ONLY_PERM);

        int i = 0;
        do {
            if (affinity.isPrimary(ignite.localNode(), ++i))
                return i;

        }
        while (i <= 1_000);

        throw new IllegalStateException(ignite.name() + " isn't primary node for any key.");
    }
}
