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

package org.apache.ignite.compatibility.clients;

import java.io.Serializable;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientPartitionAwarenessMapper;
import org.apache.ignite.client.ClientPartitionAwarenessMapperFactory;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import static org.apache.ignite.compatibility.clients.JavaThinCompatibilityTest.ADDR;
import static org.apache.ignite.compatibility.clients.JavaThinCompatibilityTest.CACHE_WITH_CUSTOM_AFFINITY;
import static org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature.ALL_AFFINITY_MAPPINGS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;

/**
 * This class is used to test a new API for partition awareness mapper factory added since 2.14 release.
 *
 * This wrapper is required to solve serialization/deserialization issues when the
 * {@link JavaThinCompatibilityTest#testClient(IgniteProductVersion, IgniteProductVersion)} is used upon previous
 * Ignite releases. The newly added classes must not be loaded by default when the test method is deserialized.
 */
public class ClientPartitionAwarenessMapperAPITestWrapper implements Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** */
    public static void testCustomPartitionAwarenessMapper() {
        X.println(">>>> Testing custom partition awareness mapper");

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(ADDR)
            .setPartitionAwarenessMapperFactory(new ClientPartitionAwarenessMapperFactory() {
                /** {@inheritDoc} */
                @Override public ClientPartitionAwarenessMapper create(String cacheName, int partitions) {
                    AffinityFunction aff = new RendezvousAffinityFunction(false, partitions);

                    return aff::partition;
                }
            });

        try (IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Integer, Integer> cache = client.cache(CACHE_WITH_CUSTOM_AFFINITY);

            assertEquals(CACHE_WITH_CUSTOM_AFFINITY, cache.getName());
            assertEquals(Integer.valueOf(0), cache.get(0));
        }
    }

    /** */
    public static void testCustomPartitionAwarenessMapperThrows() {
        X.println(">>>> Testing custom partition awareness mapper throws");

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(ADDR)
            .setPartitionAwarenessMapperFactory((cacheName, parts) -> null);

        try (IgniteClient client = Ignition.startClient(cfg)) {
            String errMsg = "Feature " + ALL_AFFINITY_MAPPINGS.name() + " is not supported by the server";

            Throwable err = assertThrowsWithCause(
                () -> client.cache(CACHE_WITH_CUSTOM_AFFINITY).get(0),
                ClientFeatureNotSupportedByServerException.class
            );

            assertEquals(errMsg, err.getMessage());
        }
    }
}
