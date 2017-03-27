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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Tests enums in queries
 */
public class IgniteSqlQueryEnumTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server");
        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.beforeTestsStarted();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //destroy all caches?
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setMarshaller(new BinaryMarshaller());

        if ("client".equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * Test that SQL tables can use enum as key
     */
    public void testSqlQueryEnumKey() throws Exception {
        CacheConfiguration<Integer, Color> ccfg = new CacheConfiguration<>();
        ccfg.setIndexedTypes(Color.class, Integer.class);
        ccfg.setName("enumkey");

        IgniteCache<Integer, Color> cache = grid("client").createCache(ccfg);

        cache.query(new SqlFieldsQuery("insert into \"enumkey\".Integer (_key, _val) values ('BlacK',0), (?, ?), (?, ?), (?, ?), (?, ?)")
                .setArgs(//"BlacK", Color.BLACK.ordinal(),
                        Color.WHITE, Color.WHITE.ordinal(),
                        Color.RED.ordinal(), Color.RED.ordinal(),
                        Color.GREEN, Color.GREEN.ordinal(),
                        Color.BLUE.toString(), Color.BLUE.ordinal()
                ));

        List<List<?>> result = cache.query(new SqlFieldsQuery("select * from \"enumkey\".Integer where _key = 1")).getAll();
        assertEquals(1, result.size());
        assertEquals(Color.WHITE, result.get(0).get(0));
        assertEquals(Color.WHITE.ordinal(), result.get(0).get(1));
    }

    /**
     * Test that SQL queries can use enums constants
     */
    public void testSqlQueryEnumValue() throws Exception {
        CacheConfiguration<Integer, Color> ccfg = new CacheConfiguration<>();
        ccfg.setIndexedTypes(Integer.class, Color.class);
        ccfg.setName("enumval");

        IgniteCache<Integer, Color> cache = grid("client").createCache(ccfg);

        cache.query(new SqlFieldsQuery("insert into \"enumval\".Color (_key, _val) values (0,'BlacK'), (?, ?), (?, ?), (?, ?), (?, ?)")
                       .setArgs(//Color.BLACK.ordinal(), "BlacK",
                             Color.WHITE.ordinal(), Color.WHITE,
                             Color.RED.ordinal(), Color.RED.ordinal(),
                             Color.GREEN.ordinal(), Color.GREEN,
                             Color.BLUE.ordinal(), Color.BLUE.toString()
                             ));

        List<List<?>> result = cache.query(new SqlFieldsQuery("select * from Color where _val = 0")).getAll();
        assertEquals(Color.BLACK.ordinal(), ((Number) result.get(0).get(0)).intValue());
        assertEquals(Color.BLACK, result.get(0).get(1));

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = 'RED'")).getAll();
        assertEquals(Color.RED.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = ?").setArgs(Color.RED.toString())).getAll();
        assertEquals(Color.RED.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = ?").setArgs(Color.RED)).getAll();
        assertEquals(Color.RED.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = ?").setArgs(Color.RED.ordinal())).getAll();
        assertEquals(Color.RED.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val in ('RED', 'GREEN', 'BLUE') order by _key asc")).getAll();
        assertEquals(Color.RED.ordinal(), ((Number) result.get(0).get(0)).intValue());
        assertEquals(Color.GREEN.ordinal(), ((Number) result.get(1).get(0)).intValue());
        assertEquals(Color.BLUE.ordinal(), ((Number) result.get(2).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val > 'RED' and _val < 'BLUE'")).getAll();
        assertEquals(Color.GREEN.ordinal(), ((Number) result.get(0).get(0)).intValue());
    }

    /**
     * Test that SQL table can use enums fields
     */
    public void testSqlQueryEnumField() throws Exception {
        checkFields(getCacheConfiguration("testSqlQueryEnumField", false, false));
    }

    /**
     * Test that SQL table can use enums field index
     */
    public void testSqlQueryEnumFieldIndex() throws Exception {
        checkFields(getCacheConfiguration("testSqlQueryEnumFieldIndex", true, false));
    }

    private CacheConfiguration<Integer, Star> getCacheConfiguration(String cfgName, boolean useIndex, boolean useBinaryConf) {
        CacheConfiguration<Integer, Star> ccfg = new CacheConfiguration<>();
        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Star.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());
        fields.put("color", Color.class.getName());

        entity.setFields(fields);

        if (useIndex) {
            ArrayList<QueryIndex> idxs = new ArrayList<>();
            QueryIndex idx = new QueryIndex("color");
            idxs.add(idx);

            entity.setIndexes(idxs);
        }

        ccfg.setQueryEntities(Collections.singleton(entity));

        ccfg.setName(cfgName);

        return ccfg;
    }

    /**
     *
     */
    private void checkFields(CacheConfiguration<Integer, Star> ccfg) throws Exception {
        IgniteCache<Integer, Star> cache = grid("client").createCache(ccfg);

        cache.query(new SqlFieldsQuery("insert into Star (_key, _val) values (?,?), (?, ?), (?, ?), (?, ?), (?, ?)")
                    .setArgs(Color.BLACK.ordinal(), new Star("hole", Color.BLACK),
                            Color.WHITE.ordinal(), new Star("hole", Color.WHITE),
                            Color.RED.ordinal(), new Star("Achenar", Color.RED),
                            Color.GREEN.ordinal(), new Star("Antares B", Color.GREEN),
                            Color.BLUE.ordinal(), new Star("Rigel", Color.BLUE)
                    ));

        List<List<?>> result = cache.query(new SqlFieldsQuery("select _key from Star where color = ?").setArgs(Color.BLUE)).getAll();
        assertEquals(Color.BLUE.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select color from Star where name='Antares B'")).getAll();
        assertEquals(Color.GREEN, result.get(0).get(0));

        result = cache.query(new SqlFieldsQuery("select _key from Star where color='GREEN'")).getAll();
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(0));


        result = cache.query(new SqlFieldsQuery("select _key from Star where color=3")).getAll();
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(0));

        //update
        cache.query(new SqlFieldsQuery("update Star set color='RED' where color=3")).getAll();
        result = cache.query(new SqlFieldsQuery("select color from Star where name='Antares B'")).getAll();
        assertEquals(Color.RED, result.get(0).get(0));
    }

    /** */
    public enum Color {
        BLACK,
        WHITE,
        RED,
        GREEN,
        BLUE
    }

    /** */
    public static class Star {
        private String name;
        private Color color;

        /** */
        public Star(String name, Color color) {
            this.name = name;
            this.color = color;
        }

        /** */
        @Override public int hashCode() {
            return name.hashCode() ^ color.hashCode();
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Star))
                return false;
            Star other = (Star)o;
            return this.name.equals(other.name) && this.color.equals(other.color);
        }
    }
}
