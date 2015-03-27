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

package org.apache.ignite.examples.java8.streaming.marketdata;

import org.apache.ignite.configuration.*;

/**
 * Configuration for the streaming caches for market data and financial instruments.
 */
public class CacheConfig {
    /**
     * Configure streaming cache for market ticks.
     */
    public static CacheConfiguration<String, Double> marketTicksCache() {
        return new CacheConfiguration<>("marketTicks");
    }

    /**
     * Configure cache for financial instruments.
     */
    public static CacheConfiguration<String, Instrument> instrumentCache() {
        CacheConfiguration<String, Instrument> instCache = new CacheConfiguration<>("instCache");

        // Index some fields for querying portfolio positions.
        instCache.setIndexedTypes(String.class, Instrument.class);

        return instCache;
    }
}
