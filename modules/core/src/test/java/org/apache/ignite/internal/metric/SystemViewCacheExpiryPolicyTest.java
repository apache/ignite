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

package org.apache.ignite.internal.metric;

import java.util.stream.Stream;
import javax.cache.configuration.Factory;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;

/** Tests for {@link CacheView} expiry policy factory representation. */
@ParameterizedClass(name = "factory={0}, actual={1}")
@MethodSource("allTypesArgs")
public class SystemViewCacheExpiryPolicyTest extends GridCommonAbstractTest {
    /** {@link Factory} instances for test with different expiry policy. */
    private static final Factory[] TTL_FACTORIES = {
        null,
        EternalExpiryPolicy.factoryOf(),
        CreatedExpiryPolicy.factoryOf(new Duration(MILLISECONDS, 100L)),
        ModifiedExpiryPolicy.factoryOf(new Duration(MILLISECONDS, 5L)),
        AccessedExpiryPolicy.factoryOf(new Duration(MINUTES, 10L)),
        new PlatformExpiryPolicyFactory(2, 4, 8),
        new PlatformExpiryPolicyFactory(1, -2, -1),
        new PlatformExpiryPolicyFactory(-1, 0, -1),
        new PlatformExpiryPolicyFactory(0, 1, -1)
    };

    /** {@link Factory} instance. */
    @Parameter(0)
    public Factory<ExpiryPolicy> factory;

    /** Anticipated {@link String} expiry policy factory representation. */
    @Parameter(1)
    public String actual;

    /**
     * @return Test parameters.
     */
    private static Stream<Arguments> allTypesArgs() {
        return Stream.of(
    Arguments.of(TTL_FACTORIES[0], "SingletonFactory [expiryPlc=EternalExpiryPolicy [create=ETERNAL]]"),
            Arguments.of(TTL_FACTORIES[1], "SingletonFactory [expiryPlc=EternalExpiryPolicy [create=ETERNAL]]"),
            Arguments.of(TTL_FACTORIES[2], "SingletonFactory [expiryPlc=CreatedExpiryPolicy [create=100 MILLISECONDS]]"),
            Arguments.of(TTL_FACTORIES[3], "SingletonFactory [expiryPlc=ModifiedExpiryPolicy [create=5 MILLISECONDS, update=5 MILLISECONDS]]"),
            Arguments.of(TTL_FACTORIES[4], "SingletonFactory [expiryPlc=AccessedExpiryPolicy [create=10 MINUTES, access=10 MINUTES]]"),
            Arguments.of(TTL_FACTORIES[5], "PlatformExpiryPolicyFactory [create=2, update=4, access=8," +
                " expiryPlc=PlatformExpiryPolicy [create=2 MILLISECONDS, update=4 MILLISECONDS, access=8 MILLISECONDS]]"),
            Arguments.of(TTL_FACTORIES[6], "PlatformExpiryPolicyFactory [create=1, update=-2, access=-1," +
                " expiryPlc=PlatformExpiryPolicy [create=1 MILLISECONDS, access=ETERNAL]]"),
            Arguments.of(TTL_FACTORIES[7], "PlatformExpiryPolicyFactory [create=-1, update=0, access=-1," +
                " expiryPlc=PlatformExpiryPolicy [create=ETERNAL, update=ZERO, access=ETERNAL]]"),
            Arguments.of(TTL_FACTORIES[8], "PlatformExpiryPolicyFactory [create=0, update=1, access=-1," +
                " expiryPlc=PlatformExpiryPolicy [create=ZERO, update=1 MILLISECONDS, access=ETERNAL]]")
        );
    }

    /**
     * Test for {@link CacheView} expiry policy factory representation. The test initializes the {@link CacheConfiguration}
     * with custom {@link PlatformExpiryPolicyFactory}. Given different ttl input, the test checks the {@link String}
     * expiry policy factory outcome for {@link CacheView#expiryPolicyFactory()}.
     */
    @Test
    public void testCacheViewExpiryPolicy() throws Exception {
        try (IgniteEx g = startGrid()) {
            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();
            ccfg.setName("cache");
            ccfg.setExpiryPolicyFactory(factory);

            g.getOrCreateCache(ccfg);

            SystemView<CacheView> caches = g.context().systemView().view(CACHES_VIEW);

            for (CacheView row : caches)
                if ("cache".equals(row.cacheName()))
                    assertEquals(actual, row.expiryPolicyFactory());
        }
    }
}
