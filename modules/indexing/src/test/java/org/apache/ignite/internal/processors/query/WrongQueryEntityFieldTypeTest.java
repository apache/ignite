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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.function.BiConsumer;
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
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/** */
@RunWith(Parameterized.class)
public class WrongQueryEntityFieldTypeTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public Supplier<?> val;

    /** */
    @Parameterized.Parameter(3)
    public String idxFld;

    /** */
    @Parameterized.Parameter(4)
    public Class<?> idxFldType;

    /** */
    @Parameterized.Parameter(5)
    public int gridCnt;

    /** */
    @Parameterized.Parameters(name = "cacheMode={0},backups={1},idxFld={3},idxFldType={4},gridCnt={5}")
    public static Collection<Object[]> parameters() {
        Supplier<?> person = WrongQueryEntityFieldTypeTest::personInside;
        Supplier<?> floatInsteadLong = WrongQueryEntityFieldTypeTest::floatInside;
        Supplier<?> organization = WrongQueryEntityFieldTypeTest::personInsideOrganization;

        Collection<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode cacheMode : CacheAtomicityMode.values()) {
            for (int backups = 0; backups < 4; backups++) {
                for (int gridCnt = 1; gridCnt < 4; gridCnt++) {
                    params.add(new Object[] {cacheMode, backups, person, "field", String.class, gridCnt});
                    params.add(new Object[] {cacheMode, backups, floatInsteadLong, "field", Long.class, gridCnt});
                    params.add(new Object[] {cacheMode, backups, organization, "head", String.class, gridCnt});
                }
            }
        }

        return params;
    }

    /** */
    private volatile boolean sysThreadFail;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                sysThreadFail = true;

                return false;
            }
        };
    }

    /** */
    @Test
    public void testPutFromThinClient() throws Exception {
        withThinClient((cli, cache) -> {
            assertThrowsWithCause(() -> cache.put(1, val.get()), ClientServerError.class);

            assertNull(cache.withKeepBinary().get(1));
        });
    }

    /** */
    @Test
    public void testPut() throws Exception {
        withNode((ign, cache) -> {
            assertThrowsWithCause(() -> cache.put(1, val.get()), CacheException.class);

            assertNull(cache.withKeepBinary().get(1));
        });
    }

    /** */
    @Test
    public void testPutFromThinClientTx() throws Exception {
        if (mode == ATOMIC)
            return;

        withThinClient((cli, cache) -> {
            for (TransactionConcurrency conc : TransactionConcurrency.values()) {
                for (TransactionIsolation iso: TransactionIsolation.values()) {
                    if (conc == OPTIMISTIC && mode == TRANSACTIONAL_SNAPSHOT)
                        continue;

                    assertThrowsWithCause(() -> {
                        try (ClientTransaction tx = cli.transactions().txStart(conc, iso)) {
                            cache.put(1, val.get());

                            tx.commit();
                        }
                    }, ClientServerError.class);

                    assertNull(cache.withKeepBinary().get(1));
                }
            }
        });
    }

    /** */
    @Test
    public void testPutTx() throws Exception {
        if (mode == ATOMIC)
            return;

        withNode((ign, cache) -> {
            for (TransactionConcurrency conc : TransactionConcurrency.values()) {
                for (TransactionIsolation iso : TransactionIsolation.values()) {
                    if (conc == OPTIMISTIC && mode == TRANSACTIONAL_SNAPSHOT)
                        continue;

                    assertThrowsWithCause(() -> {
                        try (Transaction tx = ign.transactions().txStart(conc, iso)) {
                            cache.put(1, val.get());

                            tx.commit();
                        }
                    }, mode == TRANSACTIONAL_SNAPSHOT ? CacheException.class : IgniteSQLException.class);

                    assertNull(cache.withKeepBinary().get(1));
                }
            }
        });
    }

    /** Perform action with Thin client. */
    private void withThinClient(BiConsumer<IgniteClient, ClientCache<Integer, Object>> consumer) throws Exception {
        startGrids(gridCnt);

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            ClientCache<Integer, Object> cache = cli.createCache(new ClientCacheConfiguration()
                .setName("TEST")
                .setAtomicityMode(mode)
                .setBackups(backups)
                .setQueryEntities(queryEntity()));

            consumer.accept(cli, cache);

            assertFalse(sysThreadFail);
        }
    }

    /** Perform action with Ignite node. */
    private void withNode(BiConsumer<Ignite, IgniteCache<Integer, Object>> consumer) throws Exception {
        IgniteEx ign = startGrids(gridCnt);

        IgniteCache<Integer, Object> cache = ign.createCache(new CacheConfiguration<Integer, Object>()
            .setName("TEST")
            .setAtomicityMode(mode)
            .setBackups(backups)
            .setQueryEntities(Collections.singleton(queryEntity())));

        consumer.accept(ign, cache);

        assertFalse(sysThreadFail);
    }

    /** @return Organization with the Person inside. */
    public static Object personInsideOrganization() {
        try {
            ClassLoader ldr = getExternalClassLoader();

            Class<?> organization = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Organization");
            Class<?> person = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Person");
            Class<?> address = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Address");

            Object p = person.getConstructor(String.class).newInstance("test");

            return organization.getConstructor(String.class, person, address).newInstance("org", p, null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** @return Container with the Person inside. */
    public static Object personInside() {
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
    public static Object floatInside() {
        try {
            ClassLoader ldr = getExternalClassLoader();

            Class<?> container = ldr.loadClass("org.apache.ignite.tests.p2p.cache.Container");

            return container.getConstructor(Object.class).newInstance(1f);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** @return Query entity. */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put(idxFld, idxFldType.getName()); //Actual type of the field is different.

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(val.get().getClass().getName())
            .setFields(fields)
            .setIndexes(Collections.singleton(new QueryIndex(idxFld)));
    }
}
