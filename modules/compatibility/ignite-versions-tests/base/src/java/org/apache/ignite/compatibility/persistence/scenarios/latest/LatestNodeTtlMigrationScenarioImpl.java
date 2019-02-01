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

package org.apache.ignite.compatibility.persistence.scenarios.latest;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.migration.UpgradePendingTreeToPerPartitionTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.compatibility.Since;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLatestNodeScenario;
import org.junit.Assert;

/**
 * Scenario for checking ttl migration.
 */
@Since("2.6.0")
public class LatestNodeTtlMigrationScenarioImpl implements PdsCompatibilityLatestNodeScenario {
    /** Cache name. */
    private final String cacheName;

    /** Duration sec. */
    private final int durationSec;

    /** Entries count. */
    private final int entriesCount;

    /**
     * Create an instance of LatestNodeTtlMigrationScenarioImpl.
     *
     * @param cacheName Cache name.
     * @param durationSec Duration sec.
     * @param entriesCount Entries count.
     */
    public LatestNodeTtlMigrationScenarioImpl(String cacheName, int durationSec, int entriesCount) {
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
    @Override public void validate(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.cache(cacheName);

        for (int i = 0; i < entriesCount; i++)
            Assert.assertNotNull(cache.get(i));

        final long expireTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(durationSec + 2);

        final IgniteFuture<Collection<Boolean>> upgradeTtlFut = ignite.compute().broadcastAsync(new UpgradePendingTreeToPerPartitionTask());

        // Wait till future is done and expire time is coming.
        for (;;) {
            if (upgradeTtlFut.isDone() && expireTime < System.currentTimeMillis())
                break;

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                break;
            }
        }

        for (Boolean res : upgradeTtlFut.get())
            Assert.assertTrue(res);

        for (int i = 0; i < entriesCount; i++)
            Assert.assertNull(cache.get(i));
    }
}
