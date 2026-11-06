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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Checks the thin client binary metadata cache when a channel fails while a binary object is being built.
 */
@RunWith(Parameterized.class)
public class BinaryMetadataChannelFailureTest extends AbstractThinClientTest {
    /** Type name. */
    private static final String TYPE_NAME = "TestType";

    /** Field names. */
    private static final List<String> FIELDS = Arrays.asList("f0", "f1", "f2");

    /** Field names. */
    private static final List<String> FIELDS_NEW = Arrays.asList("f0", "f1", "f2", "f3");

    /** Field whose ID resolution triggers the channel failure. */
    private static final String HOOK_FIELD = "f1";

    /** If {@code true} put a different value. */
    @Parameterized.Parameter(0)
    public Boolean differentValue;

    /** Parameters. */
    @Parameterized.Parameters(name = "differentValue = {0}")
    public static Collection<?> parameters() {
        return List.of(true, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Channel failure happens after the builder has read the cached type metadata, but before the new schema is
     * registered. The client must not end up with a cached type that lacks the fields of the registered schema.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChannelFailureDuringBuild() throws Exception {
        IgniteEx srv = startGrid(0);

        HookIdMapper idMapper = new HookIdMapper();

        ClientConfiguration cfg = getClientConfiguration(srv)
            .setBinaryConfiguration(new BinaryConfiguration().setIdMapper(idMapper));

        try (IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Integer, BinaryObject> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

            BinaryObjectBuilder builder = builder(client, FIELDS);

            // Type with all fields is registered and cached by the client.
            cache.put(0, builder.build());

            assertTrue(cache.get(0).type().fieldNames().containsAll(FIELDS));

            idMapper.hook = () -> {
                dropAllThinClientConnections(srv);

                // The next request detects the broken channel and runs the channel failure listeners.
                client.cacheNames();
            };

            if (differentValue)
                builder = builder(client, FIELDS_NEW);

            cache.put(1, builder.build());

            assertNull("Channel failure hook was not triggered", idMapper.hook);

            BinaryObject obj = cache.get(1);

            List<String> ethalon = differentValue ? FIELDS_NEW : FIELDS;

            for (String field : ethalon)
                assertEquals(field, obj.<String>field(field));

            assertTrue(srv.binary().type(TYPE_NAME).fieldNames().containsAll(ethalon));

            Collection<String> clientFields = obj.type().fieldNames();

            assertTrue("Client type metadata lacks fields of the object schema: " + clientFields,
                clientFields.containsAll(ethalon));
        }
    }

    /**
     * @param client Client.
     * @return Builder with all fields set.
     */
    private static BinaryObjectBuilder builder(IgniteClient client, List<String> fields) {
        BinaryObjectBuilder builder = client.binary().builder(TYPE_NAME);

        for (String field : fields)
            builder.setField(field, field, String.class);

        return builder;
    }

    /**
     * ID mapper that runs a one-time hook on resolving {@link #HOOK_FIELD} ID.
     * Just a simple way to reproduce concurrent disconnection during Binary Object meta processing.
     */
    private static class HookIdMapper implements BinaryIdMapper {
        /** Delegate. */
        private final BinaryIdMapper delegate = new BinaryBasicIdMapper(true);

        /** Hook. */
        private volatile Runnable hook;

        /** {@inheritDoc} */
        @Override public int typeId(String typeName) {
            return delegate.typeId(typeName);
        }

        /** {@inheritDoc} */
        @Override public int fieldId(int typeId, String fieldName) {
            Runnable hook0 = hook;

            if (hook0 != null && HOOK_FIELD.equals(fieldName)) {
                hook = null;

                hook0.run();
            }

            return delegate.fieldId(typeId, fieldName);
        }
    }
}
