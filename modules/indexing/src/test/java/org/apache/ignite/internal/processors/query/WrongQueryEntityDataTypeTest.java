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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
@RunWith(Parameterized.class)
public class WrongQueryEntityDataTypeTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean systemThreadFails;

    @Parameterized.Parameter()
    public CacheAtomicityMode cacheMode;

    @Parameterized.Parameter(1)
    public int backups;

    @Parameterized.Parameter(2)
    public Supplier<?> supplier;

    @Parameterized.Parameter(3)
    public String idxFldType;

    @Parameterized.Parameters(name = "cacheMode={0},backups={1},idxFldType={3}")
    public static Collection parameters() {
        Supplier<?> person = WrongQueryEntityDataTypeTest::personInContainer;
        Supplier<?> _float = WrongQueryEntityDataTypeTest::floatInContainer;

        return Arrays.asList(new Object[][] {
            {TRANSACTIONAL, 1, person, String.class.getName()},
            {TRANSACTIONAL, 1, _float, Long.class.getName()},
        });
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                systemThreadFails = true;

                return false;
            }
        };
    }

    /** */
    @Test
    public void testCreateWrongQueryEntityDataTypeThinClient() throws Exception {
        doWithThinClient(cache -> {
            assertThrowsWithCause(() -> cache.put(1, supplier.get()), ClientException.class);

            assertNull(cache.withKeepBinary().get(1));
        });
    }

    /** */
    @Test
    public void testCreateWrongQueryEntityDataTypeClientNode() throws Exception {
        doWithClientNode(cache -> {
            assertThrowsWithCause(() -> cache.put(1, supplier.get()), CacheException.class);

            assertNull(cache.withKeepBinary().get(1));
        });
    }

    /** */
    private void doWithThinClient(Consumer<ClientCache<Integer, Object>> consumer) throws Exception {
        systemThreadFails = false;

        startGrids(2);

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            ClientCache<Integer, Object> cache = client.createCache(new ClientCacheConfiguration()
                .setName("TEST")
                .setAtomicityMode(cacheMode)
                .setBackups(backups)
                .setQueryEntities(queryEntity()));

            consumer.accept(cache);

            assertFalse(systemThreadFails);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void doWithClientNode(Consumer<IgniteCache<Integer, Object>> consumer) throws Exception {
        systemThreadFails = false;

        IgniteEx ign = startGrids(2);

        try {
            IgniteCache<Integer, Object> cache = ign.createCache(new CacheConfiguration<Integer, Object>()
                .setName("TEST")
                .setAtomicityMode(cacheMode)
                .setBackups(backups)
                .setQueryEntities(Collections.singleton(queryEntity())));

            consumer.accept(cache);

            assertFalse(systemThreadFails);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @return Container with the Person inside. */
    public static Object personInContainer() {
        try {
            ClassLoader ldr = getExternalClassLoader();

            Class<?> container = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Container");
            Class<?> personCls = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Person");

            Object person = personCls.getConstructor().newInstance();

            return container.getConstructor(Object.class).newInstance(person);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** @return Container with the float inside. */
    public static Object floatInContainer() {
        try {
            ClassLoader ldr = getExternalClassLoader();

            Class<?> container = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Container");

            return container.getConstructor(Object.class).newInstance(1f);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        String indexedField = "head";

        fields.put("name", String.class.getName());
        fields.put(indexedField, idxFldType); //Actual type of the field Organization#head is Person.

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(supplier.get().getClass().getName())
            .setFields(fields)
            .setIndexes(Collections.singleton(new QueryIndex(indexedField)));
    }
}
