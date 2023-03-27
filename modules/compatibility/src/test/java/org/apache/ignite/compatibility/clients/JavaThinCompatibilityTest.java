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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityNodeRunner;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.resources.ServiceContextResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature.GET_SERVICE_DESCRIPTORS;
import static org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature.SERVICE_INVOKE_CALLCTX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests java thin client compatibility. This test only checks that thin client can perform basic operations with
 * different client and server versions. Whole API not checked, corner cases not checked.
 */
@RunWith(Parameterized.class)
public class JavaThinCompatibilityTest extends AbstractClientCompatibilityTest {
    /** Thin client endpoint. */
    public static final String ADDR = "127.0.0.1:10800";

    /** Cache name. */
    public static final String CACHE_WITH_CUSTOM_AFFINITY = "cache_with_custom_affinity";

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

        if (ver.compareTo(VER_2_13_0) >= 0)
            ignite.services().deployNodeSingleton("ctx_service", new CtxService());

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(CACHE_WITH_CUSTOM_AFFINITY)
            .setAffinity(new CustomAffinity()));

        cache.put(0, 0);

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

    /** {@inheritDoc} */
    @Override public void testCurrentClientToOldServer() throws Exception {
        Assume.assumeTrue("Java thin client exists only from 2.5.0 release", ver.compareTo(VER_2_5_0) >= 0);

        super.testCurrentClientToOldServer();
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

    /** */
    private void testServicesWithCallerContext() {
        X.println(">>>> Testing services with caller context");

        ServiceCallContext callCtx = ServiceCallContext.builder().put("key", "value").build();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertEquals("value", client.services().serviceProxy("ctx_service", CtxServiceInterface.class, callCtx)
                .attribute("key"));
        }
    }

    /** */
    private void testServicesWithCallerContextThrows() {
        X.println(">>>> Testing services with caller context throws");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ServiceCallContext callCtx = ServiceCallContext.builder().put("key", "value").build();

            EchoServiceInterface svc =
                client.services().serviceProxy("test_service", EchoServiceInterface.class, callCtx);

            Throwable err = assertThrowsWithCause(() -> svc.echo(1), ClientFeatureNotSupportedByServerException.class);

            assertEquals("Feature " + SERVICE_INVOKE_CALLCTX.name() + " is not supported by the server", err.getMessage());
        }
    }

    /** */
    private void testContinuousQueries() throws Exception {
        X.println(">>>> Testing continuous queries");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("testContinuousQueries");

            List<CacheEntryEvent<?, ?>> allEvts = new ArrayList<>();

            cache.query(new ContinuousQuery<>().setLocalListener(evts -> evts.forEach(allEvts::add)));

            cache.put(0, 0);
            cache.put(0, 1);
            cache.remove(0);

            assertTrue(GridTestUtils.waitForCondition(() -> allEvts.size() == 3, 10_000L));
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

        if (clientVer.compareTo(VER_2_11_0) >= 0 && serverVer.compareTo(VER_2_10_0) >= 0)
            testContinuousQueries();

        if (clientVer.compareTo(VER_2_13_0) >= 0) {
            if (serverVer.compareTo(VER_2_13_0) >= 0) {
                testServiceDescriptors();
                testServicesWithCallerContext();
            }
            else {
                testServiceDescriptorsThrows();
                testServicesWithCallerContextThrows();
            }
        }

        if (clientVer.compareTo(VER_2_15_0) >= 0)
            testDataReplicationOperations(serverVer.compareTo(VER_2_15_0) >= 0);

        if (clientVer.compareTo(VER_2_14_0) >= 0)
            new JavaThinIndexQueryCompatibilityTest().testIndexQueries(ADDR, serverVer.compareTo(VER_2_14_0) >= 0);

        if (clientVer.compareTo(VER_2_14_0) >= 0) {
            // This wrapper is used to avoid serialization/deserialization issues when the `testClient` is
            // tried to be deserialized on previous Ignite releases that do not contain a newly added classes.
            // Such classes will be loaded by classloader only if a version of the thin client is match.
            if (serverVer.compareTo(VER_2_14_0) >= 0)
                ClientPartitionAwarenessMapperAPITestWrapper.testCustomPartitionAwarenessMapper();
            else if (serverVer.compareTo(VER_2_11_0) >= 0) // Partition awareness available from.
                ClientPartitionAwarenessMapperAPITestWrapper.testCustomPartitionAwarenessMapperThrows();
        }
    }

    /** */
    private void testServiceDescriptors() {
        X.println(">>>> Testing services descriptors");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            assertEquals(2, client.services().serviceDescriptors().size());

            ClientServiceDescriptor svc = client.services().serviceDescriptor("test_service");

            assertEquals("test_service", svc.name());
            assertEquals(EchoService.class.getName(), svc.serviceClass());
            assertEquals(0, svc.totalCount());
            assertEquals(1, svc.maxPerNodeCount());
            assertNull(svc.cacheName());
            assertEquals(client.cluster().forServers().node().id(), svc.originNodeId());
            assertEquals(PlatformType.JAVA, svc.platformType());
        }
    }

    /** */
    private void testServiceDescriptorsThrows() {
        X.println(">>>> Testing services descriptors queries throws");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            String errMsg = "Feature " + GET_SERVICE_DESCRIPTORS.name() + " is not supported by the server";

            Throwable err = assertThrowsWithCause(
                () -> client.services().serviceDescriptors(),
                ClientFeatureNotSupportedByServerException.class
            );

            assertEquals(errMsg, err.getMessage());

            err = assertThrowsWithCause(
                () -> client.services().serviceDescriptor("test_service"),
                ClientFeatureNotSupportedByServerException.class
            );

            assertEquals(errMsg, err.getMessage());
        }
    }

    /** @param supported {@code True} if feature supported. */
    private void testDataReplicationOperations(boolean supported) {
        X.println(">>>> Testing cache replication");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            TcpClientCache<Object, Object> cache = (TcpClientCache<Object, Object>)client
                .getOrCreateCache("test-cache-replication");

            Map<Object, T3<Object, GridCacheVersion, Long>> puts =
                F.asMap(1, new T3<>(1, new GridCacheVersion(1, 1, 1, 2), U.currentTimeMillis() + 1000));

            Map<Object, GridCacheVersion> rmvs = F.asMap(1, new GridCacheVersion(1, 1, 1, 2));

            if (supported) {
                cache.putAllConflict(puts);

                assertEquals(1, cache.get(1));

                cache.removeAllConflict(rmvs);

                assertFalse(cache.containsKey(1));
            }
            else {
                assertThrowsWithCause(() -> cache.putAllConflict(puts), ClientFeatureNotSupportedByServerException.class);

                assertThrowsWithCause(() -> cache.removeAllConflict(rmvs), ClientFeatureNotSupportedByServerException.class);
            }
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

    /** Service for testing the attributes of a service call context. */
    public static interface CtxServiceInterface {
        /** */
        public String attribute(String name);
    }

    /** */
    public static class CtxService implements Service, CtxServiceInterface {
        /** Service context. */
        @ServiceContextResource
        private ServiceContext svcCtx;

        /** {@inheritDoc} */
        @Override public String attribute(String name) {
            ServiceCallContext callCtx = svcCtx.currentCallContext();

            return svcCtx == null ? null : callCtx.attribute(name);
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

    /** */
    public static class CustomAffinity extends RendezvousAffinityFunction {
    }

    /**
     * This class is a workaround to avoid serialization error in {@link IgniteCompatibilityNodeRunner#readClosureFromFileAndDelete}.
     * {@link IndexQuery} appeared since Ignite 2.12 and prior versions doesn't know this class, and it fails while serializing
     * {@link JavaThinCompatibilityTest} class. Then it's required to move this test case into separate class that will be loaded in
     * runtime only for the such versions that know this class.
     */
    public static class JavaThinIndexQueryCompatibilityTest {
        /** */
        public void testIndexQueries(String addr, boolean supported) {
            X.println(">>>> Testing index queries");

            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(addr))) {
                ClientCache<Object, Object> cache = client.getOrCreateCache(
                    new ClientCacheConfiguration()
                        .setName("testIndexQueries")
                        .setQueryEntities(
                            new QueryEntity()
                                .setTableName("TABLE_TEST_INDEX_QUERIES")
                                .setKeyType(Integer.class.getName())
                                .setFields(new LinkedHashMap<>(F.asMap(
                                    "A", Integer.class.getName(),
                                    "B", Integer.class.getName())))
                                .setKeyFieldName("A")
                                .setValueType("TEST_INDEX_QUERIES")
                                .setIndexes(Collections.singleton(new QueryIndex()
                                    .setName("IDX")
                                    .setFields(new LinkedHashMap<>(F.asMap("B", true)))))
                        )
                );

                cache.query(
                    new SqlFieldsQuery("insert into TABLE_TEST_INDEX_QUERIES(A, B) values (?, ?)")
                        .setArgs(0, 0)
                        .setTimeout(0, TimeUnit.MILLISECONDS))  // Required.
                    .getAll();

                IndexQuery<Integer, Object> idxQry = new IndexQuery<>("TEST_INDEX_QUERIES", "IDX");

                if (supported)
                    assertEquals(1, cache.withKeepBinary().query(idxQry).getAll().size());
                else {
                    assertThrowsWithCause(
                        () -> cache.withKeepBinary().query(idxQry).getAll(),
                        ClientFeatureNotSupportedByServerException.class);
                }
            }
        }
    }
}
