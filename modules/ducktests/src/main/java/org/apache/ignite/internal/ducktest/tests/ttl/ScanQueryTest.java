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

package org.apache.ignite.internal.ducktest.tests.ttl;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Java client. Scan Query operation
 */
public class ScanQueryTest extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cache_name").asText();

        long entryCnt = Optional.ofNullable(jsonNode.get("entry_count"))
            .map(JsonNode::asLong)
            .orElse(100000L);

        int entrySize = Optional.ofNullable(jsonNode.get("entry_size"))
            .map(JsonNode::asInt)
            .orElse(10);

        long pacing = Optional.ofNullable(jsonNode.get("pacing"))
            .map(JsonNode::asLong)
            .orElse(1000L);

        String ttl = Optional.ofNullable(jsonNode.get("ttl"))
            .map(JsonNode::asText)
            .orElse("touched");

        CacheConfiguration<Integer, byte[]> cfg = new CacheConfiguration<Integer, byte[]>(cacheName)
            .setStatisticsEnabled(true)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setExpiryPolicyFactory(getExpirePolicy(ttl));

        IgniteCache<Integer, byte[]> cache = ignite.createCache(cfg);

        try (IgniteDataStreamer<Integer, byte[]> strm = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < entryCnt; i++)
                strm.addData(i, new byte[entrySize]);
        }

        markInitialized();

        while (!terminated()) {
            long startTime = System.nanoTime();

            cache.query(new ScanQuery<>()).forEach((v) -> {});

            long resultTime = System.nanoTime() - startTime;

            Thread.sleep(Math.max(0, pacing * 1_000_000 - resultTime) / 1_000_000);
        }

        markFinished();
    }

    /** */
    private Factory<? extends ExpiryPolicy> getExpirePolicy(String ttl) {
        Factory<? extends ExpiryPolicy> factory = null;

        switch (ttl) {
            case "touched":
                factory = TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 60));
                break;
            case "accessed":
                factory = AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 60));
                break;
        }

        return factory;
    }
}
