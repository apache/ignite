/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence.scenarios.legacy;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLegacyNodeScenario;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Assert;

/**
 * Scenario for checking ttl migration.
 */
public class LegacyNodeTtlMigrationScenarioImpl implements PdsCompatibilityLegacyNodeScenario {
    /** Cache name. */
    private final String cacheName;

    /** Duration sec. */
    private final int durationSec;

    /** Entries count. */
    private final int entriesCount;

    /**
     * Creates an instance of LegacyNodeTtlMigrationScenarioImpl.
     *
     * @param cacheName Cache name.
     * @param durationSec Duration sec.
     * @param entriesCount Entries count.
     */
    public LegacyNodeTtlMigrationScenarioImpl(String cacheName, int durationSec, int entriesCount) {
        this.cacheName = cacheName;
        this.durationSec = durationSec;
        this.entriesCount = entriesCount;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configure() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(cacheName)
            .setName(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, durationSec)))
            .setEagerTtl(true)
            .setGroupName("myGroup");

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void run(Ignite ignite) throws IgniteCheckedException {
        IgniteCache<Object, Object> cache = ignite.cache(cacheName);

        for (int i = 0; i < entriesCount; i++)
            cache.put(i, "data-" + i);

        //Touch
        for (int i = 0; i < entriesCount; i++)
            Assert.assertNotNull(cache.get(i));

        System.out.println("Data loading is finished.");
    }
}
