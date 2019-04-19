/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for JSR-310 Java 8 Date and Time API queries tests.
 */
public abstract class CacheQueryJsr310Java8DateTimeApiAbstractTest extends GridCommonAbstractTest {
    /** {@link LocalTime} instance. */
    protected static final LocalTime LOCAL_TIME = LocalTime.now().minusHours(10);

    /**
     *  The number of days subtracted from the current time when constructing
     *  {@link LocalDate} and {@link LocalDateTime}
     *  instances.
     */
    protected static final long DAYS_BEFORE_NOW = 10;

    /** {@link LocalDate} instance. */
    protected static final LocalDate LOCAL_DATE = LocalDate.now().minusDays(DAYS_BEFORE_NOW);

    /** {@link LocalDateTime} instance. */
    protected static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(LOCAL_DATE, LocalTime.MIDNIGHT);

    /**
     * Creates a cache configuration with the specified cache name
     * and indexed type key/value pairs.
     *
     * @param cacheName Cache name
     * @param indexedTypes key/value pairs according to {@link CacheConfiguration#setIndexedTypes(Class[])}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Cache configuration.
     */
    protected static <K, V> CacheConfiguration<K, V> createCacheConfig(String cacheName, Class<?>... indexedTypes) {
        return new CacheConfiguration<K, V>(cacheName)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(indexedTypes);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
