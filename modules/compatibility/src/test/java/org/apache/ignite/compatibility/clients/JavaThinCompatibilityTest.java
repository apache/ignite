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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests java thin client compatibility. This test only checks that thin client can perform basic operations with
 * different client and server versions. Whole API not checked, corner cases not checked.
 */
@RunWith(Parameterized.class)
public class JavaThinCompatibilityTest extends AbstractClientCompatibilityTest {
    /** Thin client endpoint. */
    private static final String ADDR = "127.0.0.1:10800";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1)
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected void initNode(Ignite ignite) {
        ignite.services().deployNodeSingleton("test_service", new EchoService());

        super.initNode(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void processRemoteConfiguration(IgniteConfiguration cfg) {
        super.processRemoteConfiguration(cfg);

        if (ver.compareTo(VER_2_9_0) >= 0) {
            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(1)));
        }
    }

    /** {@inheritDoc} */
    @Override public void testOldClientToCurrentServer() throws Exception {
        Assume.assumeTrue("Java thin client exists only from 2.5.0 release", ver.compareTo(VER_2_5_0) >= 0);

        super.testOldClientToCurrentServer();
    }

    /** */
    private void testCacheConfiguration(
        boolean checkFieldsPrecessionAndScale,
        boolean checkExpiryPlc
    ) throws Exception {
        X.println(">>>> Testing cache configuration");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            String cacheName = "testCacheConfiguration";

            ClientCacheConfiguration ccfg = new ClientCacheConfiguration();
            ccfg.setName(cacheName);
            ccfg.setBackups(3);
            ccfg.setGroupName("cache");
            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity qryEntity = new QueryEntity(int.class.getName(), "Entity")
                .setTableName("ENTITY")
                .setFields(new LinkedHashMap<>(
                    F.asMap("id", Integer.class.getName(), "rate", Double.class.getName())));

            if (checkFieldsPrecessionAndScale) {
                qryEntity.setFieldsPrecision(F.asMap("rate", 5));
                qryEntity.setFieldsScale(F.asMap("rate", 2));
            }

            ccfg.setQueryEntities(qryEntity);

            if (checkExpiryPlc)
                ccfg.setExpiryPolicy(new PlatformExpiryPolicy(10, 20, 30));

            client.createCache(ccfg);

            ClientCacheConfiguration ccfg1 = client.cache(cacheName).getConfiguration();

            assertEquals(ccfg.getName(), ccfg1.getName());
            assertEquals(ccfg.getBackups(), ccfg1.getBackups());
            assertEquals(ccfg.getGroupName(), ccfg1.getGroupName());
            assertEquals(ccfg.getCacheMode(), ccfg1.getCacheMode());
            assertEquals(ccfg.getQueryEntities().length, ccfg1.getQueryEntities().length);
            assertEquals(ccfg.getQueryEntities()[0].getTableName(), ccfg1.getQueryEntities()[0].getTableName());
            assertEquals(ccfg.getQueryEntities()[0].getFields(), ccfg1.getQueryEntities()[0].getFields());

            if (checkFieldsPrecessionAndScale) {
                assertEquals(ccfg.getQueryEntities()[0].getFieldsPrecision(),
                    ccfg1.getQueryEntities()[0].getFieldsPrecision());
                assertEquals(ccfg.getQueryEntities()[0].getFieldsScale(), ccfg1.getQueryEntities()[0].getFieldsScale());
            }

            if (checkExpiryPlc) {
                assertEquals(ccfg.getExpiryPolicy().getExpiryForCreation(),
                    ccfg1.getExpiryPolicy().getExpiryForCreation());
                assertEquals(ccfg.getExpiryPolicy().getExpiryForAccess(), ccfg1.getExpiryPolicy().getExpiryForAccess());
                assertEquals(ccfg.getExpiryPolicy().getExpiryForUpdate(), ccfg1.getExpiryPolicy().getExpiryForUpdate());
            }
        }
    }

    /** */
    private void testCacheApi() throws Exception {
        X.println(">>>> Testing cache API");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("testCacheApi");

            cache.put(1, 1);

            assertEquals(1, cache.get(1));

            Person person = new Person(2, "name");

            cache.put(2, person);

            assertEquals(person, cache.get(2));
        }
    }

    /** */
    private void testAuthentication() throws Exception {
        X.println(">>>> Testing authentication");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR)
            .setUserName("user").setUserPassword("password"))) {
            assertNotNull(client);
        }

    }

    /** */
    private void testTransactions() throws Exception {
        X.println(">>>> Testing transactions");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ClientCache<Object, Object> cache = client.getOrCreateCache(new ClientCacheConfiguration()
                .setName("testTransactions")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            );

            try (ClientTransaction tx = client.transactions().txStart()) {
                cache.put(1, 1);
                cache.put(2, 2);

                tx.commit();
            }

            assertEquals(1, cache.get(1));
            assertEquals(2, cache.get(2));
        }
    }

    /** */
    private void testBinary() throws Exception {
        X.println(">>>> Testing binary");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            IgniteBinary binary = client.binary();

            BinaryObject val = binary.builder("Person")
                .setField("id", 1, int.class)
                .setField("name", "Joe", String.class)
                .build();

            ClientCache<Object, BinaryObject> cache = client.getOrCreateCache("testBinary").withKeepBinary();

            cache.put(0, val);

            BinaryObject cachedVal = cache.get(0);

            assertEquals(val, cachedVal);
        }
    }

    /** */
    private void testQueries() throws Exception {
        X.println(">>>> Testing queries");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("testQueries");

            cache.put(1, 1);

            List<Cache.Entry<Object, Object>> res = cache.query(new ScanQuery<>()).getAll();

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).getKey());
            assertEquals(1, res.get(0).getValue());
        }
    }

    /** */
    private void testExpiryPolicy() throws Exception {
        X.println(">>>> Testing expiry policy");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("testExpiryPolicy");
            cache = cache.withExpirePolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)));

            cache.put(1, 1);

            doSleep(10);

            assertFalse(cache.containsKey(1));
        }
    }

    /** */
    private void testUserAttributes() throws Exception {
        X.println(">>>> Testing user attributes");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR)
            .setUserAttributes(F.asMap("attr", "val")))) {
            assertNotNull(client);
        }
    }

    /** */
    private void testClusterAPI() throws Exception {
        X.println(">>>> Testing cluster API");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertTrue(client.cluster().state().active());
        }
    }

    /** */
    private void testClusterGroups() throws Exception {
        X.println(">>>> Testing cluster groups");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertEquals(1, client.cluster().forServers().nodes().size());
        }
    }

    /** */
    private void testCompute() throws Exception {
        X.println(">>>> Testing compute");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertEquals((Integer)1, client.compute().execute(EchoTask.class.getName(), 1));
        }
    }

    /** */
    private void testServices() throws Exception {
        X.println(">>>> Testing services");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertEquals(1, client.services().serviceProxy("test_service", EchoServiceInterface.class)
                .echo(1));
        }
    }

    /** {@inheritDoc} */
    @Override protected void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer) throws Exception {
        IgniteProductVersion minVer = clientVer.compareTo(serverVer) < 0 ? clientVer : serverVer;

        testCacheConfiguration(
            minVer.compareTo(VER_2_7_0) >= 0,
            minVer.compareTo(VER_2_8_0) >= 0
        );

        testCacheApi();

        testBinary();

        testQueries();

        if (minVer.compareTo(VER_2_5_0) >= 0)
            testAuthentication();

        if (minVer.compareTo(VER_2_8_0) >= 0) {
            testTransactions();
            testExpiryPolicy();
        }

        if (clientVer.compareTo(VER_2_9_0) >= 0 && serverVer.compareTo(VER_2_8_0) >= 0)
            testClusterAPI();

        if (minVer.compareTo(VER_2_9_0) >= 0) {
            testUserAttributes();
            testClusterGroups();
            testCompute();
            testServices();
        }
    }

    /** */
    public static interface EchoServiceInterface {
        /** */
        public int echo(int val);
    }

    /** */
    public static class EchoService implements Service, EchoServiceInterface {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int echo(int val) {
            return val;
        }
    }

    /** */
    public static class EchoJob implements ComputeJob {
        /** Value. */
        private final Integer val;

        /**
         * @param val Value.
         */
        public EchoJob(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return val;
        }
    }

    /** */
    public static class EchoTask extends ComputeTaskAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Integer arg) throws IgniteException {
            return F.asMap(new EchoJob(arg), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.get(0).getData();
        }
    }
}
