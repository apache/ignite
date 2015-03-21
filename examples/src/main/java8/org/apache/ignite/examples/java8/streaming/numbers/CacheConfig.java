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

package org.apache.ignite.examples.java8.streaming.numbers;

import org.apache.ignite.configuration.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Configuration for the streaming cache to store the stream of random numbers.
 * This cache is configured with sliding window of 1 second, which means that
 * data older than 1 second will be automatically removed from the cache.
 */
public class CacheConfig {
    /**
     * Configure streaming cache.
     */
    public static CacheConfiguration<Integer, Long> randomNumbersCache() {
        CacheConfiguration<Integer, Long> cfg = new CacheConfiguration<>("randomNumbers");

        cfg.setIndexedTypes(Integer.class, Long.class);

        // Sliding window of 1 seconds.
        cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, 1))));

        return cfg;
    }
}
