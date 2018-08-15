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

package org.apache.ignite.internal.processors.cache.index;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public abstract class DynamicIndexAbstractSelfTest extends AbstractSchemaSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Attribute to filter node out of cache data nodes. */
    protected static final String ATTR_FILTERED = "FILTERED";

    /** Key range limit for "before" step. */
    protected static final int KEY_BEFORE = 100;

    /** Key range limit for "after" step. */
    protected static final int KEY_AFTER = 200;

    /** SQL to check index on the field 1. */
    protected static final String SQL_SIMPLE_FIELD_1 = "SELECT * FROM " + TBL_NAME + " WHERE " + FIELD_NAME_1_ESCAPED + " >= ?";

    /** SQL to check composite index */
    protected static final String SQL_COMPOSITE = "SELECT * FROM " + TBL_NAME + " WHERE " + FIELD_NAME_1_ESCAPED +
        " >= ? AND " + alias(FIELD_NAME_2_ESCAPED) + " >= ?";

    /** SQL to check index on the field 2. */
    protected static final String SQL_SIMPLE_FIELD_2 =
        "SELECT * FROM " + TBL_NAME + " WHERE " + alias(FIELD_NAME_2_ESCAPED) + " >= ?";

    /** Argument for simple SQL (1). */
    protected static final int SQL_ARG_1 = 40;

    /** Argument for simple SQL (2). */
    protected static final int SQL_ARG_2 = 80;

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return serverConfiguration(idx, false);
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @param filter Whether to filter the node out of cache.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration serverConfiguration(int idx, boolean filter) throws Exception {
        IgniteConfiguration cfg = commonConfiguration(idx);

        if (filter)
            cfg.setUserAttributes(Collections.singletonMap(ATTR_FILTERED, true));

        return cfg;
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setMarshaller(new BinaryMarshaller());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(128L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        return optimize(cfg);
    }

    /**
     * @return Default cache configuration.
     */
    protected CacheConfiguration<KeyClass, ValueClass> cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(KeyClass.class.getName());
        entity.setValueType(ValueClass.class.getName());

        entity.setKeyFieldName(FIELD_KEY_ALIAS);
        entity.addQueryField(FIELD_KEY_ALIAS, entity.getKeyType(), null);

        entity.addQueryField(FIELD_KEY, Long.class.getName(), null);
        entity.addQueryField(FIELD_NAME_1_ESCAPED, Long.class.getName(), null);
        entity.addQueryField(FIELD_NAME_2_ESCAPED, Long.class.getName(), null);

        entity.setKeyFields(Collections.singleton(FIELD_KEY));

        entity.setAliases(Collections.singletonMap(FIELD_NAME_2_ESCAPED, alias(FIELD_NAME_2_ESCAPED)));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        ccfg.setNodeFilter(new NodeFilter());

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @return Local cache configuration with a pre-configured index.
     */
    CacheConfiguration<KeyClass, ValueClass> localCacheConfiguration() {
        CacheConfiguration<KeyClass, ValueClass> res = cacheConfiguration();

        res.getQueryEntities().iterator().next().setIndexes(Collections.singletonList(
            new QueryIndex(FIELD_NAME_2_ESCAPED, true, IDX_NAME_LOCAL)));

        res.setCacheMode(CacheMode.LOCAL);

        return res;
    }

    /**
     * Ensure index is used in plan.
     *
     * @param idxName Index name.
     * @param sql SQL.
     * @param args Arguments.
     */
    protected static void assertIndexUsed(String idxName, String sql, Object... args) {
        for (Ignite node : Ignition.allGrids())
            assertIndexUsed((IgniteEx)node, idxName, sql, args);
    }

    /**
     * Ensure index is used in plan.
     *
     * @param node Node.
     * @param idxName Index name.
     * @param sql SQL.
     * @param args Arguments.
     */
    protected static void assertIndexUsed(IgniteEx node, String idxName, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery("EXPLAIN " + sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        String plan = (String)node.cache(CACHE_NAME).query(qry).getAll().get(0).get(0);

        assertTrue("Index is not used: " + plan, plan.toLowerCase().contains(idxName.toLowerCase()));
    }

    /**
     * Ensure index is not used in plan.
     *
     * @param idxName Index name.
     * @param sql SQL.
     * @param args Arguments.
     */
    protected static void assertIndexNotUsed(String idxName, String sql, Object... args) {
        for (Ignite node : Ignition.allGrids())
            assertIndexNotUsed((IgniteEx)node, idxName, sql, args);
    }

    /**
     * Ensure index is not used in plan.
     *
     * @param node Node.
     * @param idxName Index name.
     * @param sql SQL.
     * @param args Arguments.
     */
    protected static void assertIndexNotUsed(IgniteEx node, String idxName, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery("EXPLAIN " + sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        String plan = (String)node.cache(CACHE_NAME).query(qry).getAll().get(0).get(0);

        assertFalse("Index is used: " + plan, plan.contains(idxName));
    }

    /**
     * Create key object.
     *
     * @param ignite Ignite instance.
     * @param id ID.
     * @return Key object.
     */
    protected static BinaryObject key(Ignite ignite, long id) {
        return ignite.binary().builder(KeyClass.class.getName()).setField(FIELD_KEY, id).build();
    }

    /**
     * Create value object.
     *
     * @param ignite Ignite instance.
     * @param id ID.
     * @return Value object.
     */
    protected static BinaryObject value(Ignite ignite, long id) {
        return ignite.binary().builder(ValueClass.class.getName())
            .setField(FIELD_NAME_1_ESCAPED, id)
            .setField(FIELD_NAME_2_ESCAPED, id)
            .build();
    }

    /**
     * Create key/value entry for the given key.
     *
     * @param ignite Ignite instance.
     * @param id ID.
     * @return Entry.
     */
    protected static T2<BinaryObject, BinaryObject> entry(Ignite ignite, long id) {
        return new T2<>(key(ignite, id), value(ignite, id));
    }

    /**
     * Get common cache.
     *
     * @param node Node.
     * @return Cache.
     */
    protected static IgniteCache<BinaryObject, BinaryObject> cache(Ignite node) {
        return node.cache(CACHE_NAME).withKeepBinary();
    }

    /**
     * Get key.
     *
     * @param node Node.
     * @param id ID.
     */
    protected static BinaryObject get(Ignite node, int id) {
        BinaryObject key = key(node, id);

        return cache(node).get(key);
    }

    /**
     * Put key range.
     *
     * @param node Node.
     * @param from From key.
     * @param to To key.
     */
    protected static void put(Ignite node, int from, int to) {
        try (IgniteDataStreamer streamer = node.dataStreamer(CACHE_NAME)) {
            streamer.allowOverwrite(true);
            streamer.keepBinary(true);

            for (int i = from; i < to; i++) {
                BinaryObject key = key(node, i);
                BinaryObject val = value(node, i);

                streamer.addData(key, val);
            }

            streamer.flush();
        }
    }

    /**
     * Put key to cache.
     *
     * @param node Node.
     * @param id ID.
     */
    protected static void put(Ignite node, long id) {
        BinaryObject key = key(node, id);
        BinaryObject val = value(node, id);

        cache(node).put(key, val);
    }

    /**
     * Remove key range.
     *
     * @param node Node.
     * @param from From key.
     * @param to To key.
     */
    protected static void remove(Ignite node, int from, int to) {
        for (int i = from; i < to; i++)
            remove(node, i);
    }

    /**
     * Remove key form cache.
     *
     * @param node Node.
     * @param id ID.
     */
    protected static void remove(Ignite node, long id) {
        BinaryObject key = key(node, id);

        cache(node).remove(key);
    }

    /**
     * @return Random string.
     */
    protected static String randomString() {
        return "random" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Assert SQL simple data state.
     *
     * @param sql SQL query.
     * @param expSize Expected size.
     */
    protected static void assertSqlSimpleData(String sql, int expSize) {
        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData(node, sql, expSize);
    }

    /**
     * Assert SQL simple data state.
     *
     * @param node Node.
     * @param sql SQL query.
     * @param expSize Expected size.
     */
    protected static void assertSqlSimpleData(Ignite node, String sql, int expSize) {
        SqlQuery qry = new SqlQuery(typeName(ValueClass.class), sql).setArgs(SQL_ARG_1);

        List<Cache.Entry<BinaryObject, BinaryObject>> res = node.cache(CACHE_NAME).withKeepBinary().query(qry).getAll();

        Set<Long> ids = new HashSet<>();

        for (Cache.Entry<BinaryObject, BinaryObject> entry : res) {
            long id = entry.getKey().field(FIELD_KEY);

            long field1 = entry.getValue().field(FIELD_NAME_1_ESCAPED);
            long field2 = entry.getValue().field(FIELD_NAME_2_ESCAPED);

            assertTrue(field1 >= SQL_ARG_1);

            assertEquals(id, field1);
            assertEquals(id, field2);

            assertTrue(ids.add(id));
        }

        assertEquals("Size mismatch [node=" + node.name() + ", exp=" + expSize + ", actual=" + res.size() +
            ", ids=" + ids + ']', expSize, res.size());
    }

    /**
     * Assert SQL simple data state.
     *
     * @param node Node.
     * @param sql SQL query.
     * @param expSize Expected size.
     */
    protected static void assertSqlCompositeData(Ignite node, String sql, int expSize) {
        SqlQuery qry = new SqlQuery(typeName(ValueClass.class), sql).setArgs(SQL_ARG_1, SQL_ARG_2);

        List<Cache.Entry<BinaryObject, BinaryObject>> res = node.cache(CACHE_NAME).withKeepBinary().query(qry).getAll();

        Set<Long> ids = new HashSet<>();

        for (Cache.Entry<BinaryObject, BinaryObject> entry : res) {
            long id = entry.getKey().field(FIELD_KEY);

            long field1 = entry.getValue().field(FIELD_NAME_1_ESCAPED);
            long field2 = entry.getValue().field(FIELD_NAME_2_ESCAPED);

            assertTrue(field1 >= SQL_ARG_2);

            assertEquals(id, field1);
            assertEquals(id, field2);

            assertTrue(ids.add(id));
        }

        assertEquals("Size mismatch [exp=" + expSize + ", actual=" + res.size() + ", ids=" + ids + ']',
            expSize, res.size());
    }

    /**
     * Node filter.
     */
    protected static class NodeFilter implements IgnitePredicate<ClusterNode>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(ATTR_FILTERED) == null;
        }
    }
}
