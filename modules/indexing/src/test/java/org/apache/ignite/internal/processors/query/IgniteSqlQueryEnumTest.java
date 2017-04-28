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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.F;
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

    /** Name of enum which has no class, but configuration */
    private static final String CFG_ENUM_NAME = "Color";

    /** Cache with enum as key */
    private static final String CACHE_NAME_ENUM_KEY = "enum_key";

    /** Cache with enum as value */
    private static final String CACHE_NAME_ENUM_VAL = "enum_val";

    /** Cache with enum as field */
    private static final String CACHE_NAME_ENUM_FIELD = "enum_field";

    /** Cache with enum as indexed field */
    private static final String CACHE_NAME_ENUM_FIELD_IDX = "enum_field_idx";

    /** Cache with cfg enum as key */
    private static final String CACHE_NAME_CFG_ENUM_KEY = "cfg_enum_key";

    /** Cache with cfg enum as value */
    private static final String CACHE_NAME_CFG_ENUM_VAL = "cfg_enum_val";

    /** Cache with cfg enum as field */
    private static final String CACHE_NAME_CFG_ENUM_FIELD = "cfg_enum_field";

    /** Cache with cfg enum as indexed field */
    private static final String CACHE_NAME_CFG_ENUM_FIELD_IDX = "cfg_enum_field_idx";


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(4);

        startGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.beforeTestsStarted();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setMarshaller(new BinaryMarshaller());

        BinaryConfiguration binCfg = new BinaryConfiguration();

        BinaryTypeConfiguration binEnumCfg = new BinaryTypeConfiguration(CFG_ENUM_NAME);
        binEnumCfg.setEnumValues(Color.NAMES);

        binCfg.setTypeConfigurations(F.asList(binEnumCfg));
        cfg.setBinaryConfiguration(binCfg);

        cfg.setCacheConfiguration(
                getCacheConfiguration(CACHE_NAME_ENUM_KEY),
                getCacheConfiguration(CACHE_NAME_ENUM_VAL),
                getCacheConfiguration(CACHE_NAME_ENUM_FIELD),
                getCacheConfiguration(CACHE_NAME_ENUM_FIELD_IDX),
                getCacheConfiguration(CACHE_NAME_CFG_ENUM_KEY),
                getCacheConfiguration(CACHE_NAME_CFG_ENUM_VAL),
                getCacheConfiguration(CACHE_NAME_CFG_ENUM_FIELD),
                getCacheConfiguration(CACHE_NAME_CFG_ENUM_FIELD_IDX));


        if ("client".equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    private CacheConfiguration getCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration<>(name);
        if (name.equals(CACHE_NAME_ENUM_KEY))
            ccfg.setIndexedTypes(Color.class, Integer.class);
        else if (name.equals(CACHE_NAME_ENUM_VAL))
            ccfg.setIndexedTypes(Integer.class, Color.class);
        else if (name.equals(CACHE_NAME_CFG_ENUM_KEY)) {
            QueryEntity qe = new QueryEntity(CFG_ENUM_NAME, Integer.class.getName());
            ccfg.setQueryEntities(F.asList(qe));
        }
        else if (name.equals(CACHE_NAME_CFG_ENUM_VAL)) {
            QueryEntity qe = new QueryEntity(Integer.class.getName(), CFG_ENUM_NAME);
            ccfg.setQueryEntities(F.asList(qe));
        }
        else if (name.equals(CACHE_NAME_ENUM_FIELD) ||
                name.equals(CACHE_NAME_ENUM_FIELD_IDX) ||
                name.equals(CACHE_NAME_CFG_ENUM_FIELD) ||
                name.equals(CACHE_NAME_CFG_ENUM_FIELD_IDX)) {
            QueryEntity entity = new QueryEntity();
            entity.setKeyType(Integer.class.getName());

            if (name.equals(CACHE_NAME_CFG_ENUM_FIELD) ||
                name.equals(CACHE_NAME_CFG_ENUM_FIELD_IDX))
                entity.setValueType(Object.class.getName());
            else
                entity.setValueType(Star.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("name", String.class.getName());

            if (name.equals(CACHE_NAME_CFG_ENUM_FIELD) ||
                name.equals(CACHE_NAME_CFG_ENUM_FIELD_IDX))
                fields.put("color", CFG_ENUM_NAME);
            else
                fields.put("color", Color.class.getName());

            entity.setFields(fields);

            if (name.equals(CACHE_NAME_ENUM_FIELD_IDX) ||
                name.equals(CACHE_NAME_CFG_ENUM_FIELD_IDX)) {
                ArrayList<QueryIndex> idxs = new ArrayList<>();
                QueryIndex idx = new QueryIndex("color");
                idxs.add(idx);

                entity.setIndexes(idxs);
            }

            ccfg.setQueryEntities(Collections.singleton(entity));
        }

        return ccfg;
    }

    /**
     * Test that SQL tables can use enum as key
     */
    public void testSqlQueryEnumKey() throws Exception {
        IgniteCache<Color, Integer> cache = grid("client").cache(CACHE_NAME_ENUM_KEY);

        for (Color color : Color.values())
            cache.put(color, color.ordinal());

        List<List<?>> result = cache.query(new SqlFieldsQuery("select * from Integer where _key = ?")
                    .setArgs(Color.RED.ordinal())).getAll();

        assertEquals(1, result.size());
        assertEquals(Color.RED, result.get(0).get(0));
        assertEquals(Color.RED.ordinal(), result.get(0).get(1));

        result = cache.query(new SqlFieldsQuery("select * from Integer where _key = ?")
                    .setArgs(Color.GREEN.name())).getAll();

        assertEquals(1, result.size());
        assertEquals(Color.GREEN, result.get(0).get(0));
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(1));

        result = cache.query(new SqlFieldsQuery("select * from Integer where _key = 'BLACK'")).getAll();
        assertEquals(1, result.size());
        assertEquals(Color.BLACK, result.get(0).get(0));
        assertEquals(Color.BLACK.ordinal(), result.get(0).get(1));
    }

    /**
     * Test that SQL queries can use enums constants
     */
    public void testSqlQueryEnumValue() throws Exception {
        IgniteCache<Integer, Color> cache = grid("client").cache(CACHE_NAME_ENUM_VAL);

        for (Color color : Color.values())
            cache.put(color.ordinal(), color);

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
        checkFields(CACHE_NAME_ENUM_FIELD);
    }

    /**
     * Test that SQL table can use enums field index
     */
    public void testSqlQueryEnumFieldIndex() throws Exception {
        checkFields(CACHE_NAME_ENUM_FIELD_IDX);
    }

    /**
     * Test that SQL queries can use enums constants
     */
    public void testSqlQueryEnumValueUnavailableClass() throws Exception {
        IgniteCache cache = grid("client").cache(CACHE_NAME_CFG_ENUM_VAL).withKeepBinary();

        cache.query(new SqlFieldsQuery("insert into Color (_key, _val) values (0, 'BLACK'), (?, ?), (?, ?), (?, ?), (?, ?)")
                .setArgs(Color.WHITE.ordinal(), Color.WHITE.ordinal(),
                        Color.RED.ordinal(), Color.RED.name(),
                        Color.GREEN.ordinal(), Color.GREEN.ordinal(),
                        Color.BLUE.ordinal(), Color.BLUE.ordinal()
                ));

        List<List<?>> result = cache.query(new SqlFieldsQuery("select * from Color where _val = 0")).getAll();
        assertEquals(Color.BLACK.ordinal(), result.get(0).get(0));
        assertEquals(Color.BLACK.ordinal(), ((BinaryObject)result.get(0).get(1)).enumOrdinal());

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = 'RED'")).getAll();
        assertEquals(Color.RED.ordinal(), result.get(0).get(0));

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = ?").setArgs(Color.RED.toString())).getAll();
        assertEquals(Color.RED.ordinal(), result.get(0).get(0));

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val = ?").setArgs(Color.RED.ordinal())).getAll();
        assertEquals(Color.RED.ordinal(), result.get(0).get(0));

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val in ('RED', 'GREEN', 'BLUE') order by _key asc")).getAll();
        assertEquals(Color.RED.ordinal(), result.get(0).get(0));
        assertEquals(Color.GREEN.ordinal(), result.get(1).get(0));
        assertEquals(Color.BLUE.ordinal(), result.get(2).get(0));

        result = cache.query(new SqlFieldsQuery("select _key from Color where _val > 'RED' and _val < 'BLUE'")).getAll();
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(0));
    }

    /**
     * Test that SQL table can use enums fields
     */
    public void testSqlQueryEnumFieldUnavailableClass() throws Exception {
        IgniteCache<Integer, Object> cache = grid("client").cache(CACHE_NAME_CFG_ENUM_FIELD).withKeepBinary();

        cache.query(new SqlFieldsQuery("insert into Object (_key, name, color) values (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)")
                .setArgs(Color.BLACK.ordinal(), "Black Hole", Color.BLACK.name(),
                        Color.WHITE.ordinal(), "White Hole", Color.WHITE.ordinal(),
                        Color.RED.ordinal(), "Achenar", Color.RED.name(),
                        Color.GREEN.ordinal(), "Antares B", Color.GREEN.ordinal(),
                        Color.BLUE.ordinal(), "Rigel", Color.BLUE.ordinal()
                ));

        List<List<?>> result = cache.query(new SqlFieldsQuery("select _key from Object where color = ?").setArgs(Color.BLUE.name())).getAll();
        assertEquals(Color.BLUE.ordinal(), ((Number) result.get(0).get(0)).intValue());

        result = cache.query(new SqlFieldsQuery("select color from Object where name='Antares B'")).getAll();
        assertEquals(Color.GREEN.ordinal(), ((BinaryObject)result.get(0).get(0)).enumOrdinal());

        result = cache.query(new SqlFieldsQuery("select _key from Object where color='GREEN'")).getAll();
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(0));


        result = cache.query(new SqlFieldsQuery("select _key from Object where color=3")).getAll();
        assertEquals(Color.GREEN.ordinal(), result.get(0).get(0));

        //update
        cache.query(new SqlFieldsQuery("update Object set color='RED' where color=3")).getAll();
        result = cache.query(new SqlFieldsQuery("select color from Object where name='Antares B'")).getAll();
        assertEquals(Color.RED.ordinal(), ((BinaryObject)result.get(0).get(0)).enumOrdinal());

    }

    /** */
    private void checkFields(String cacheName) throws Exception {
        IgniteCache<Integer, Star> cache = grid("client").cache(cacheName);

        cache.query(new SqlFieldsQuery("insert into Star (_key, _val) values (?,?), (?, ?), (?, ?), (?, ?), (?, ?)")
                .setArgs(Color.BLACK.ordinal(), new Star("Black Hole", Color.BLACK),
                        Color.WHITE.ordinal(), new Star("White Hole", Color.WHITE),
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

    /**
     * @param inEnumClass Enum class.
     * @param <T> Enum type parameter.
     * @return Array of enum names.
     */
    private static <T extends Enum<?>> String[] getEnumNames(Class<T> inEnumClass){
        T [] values = inEnumClass.getEnumConstants();
        int len = values.length;
        String[] names = new String[len];
        for(int i=0;i<values.length;i++){
            names[i] = values[i].name();
        }
        return names;
    }

    /** */
    public enum Color {
        BLACK,
        WHITE,
        RED,
        GREEN,
        BLUE;

        public static final String[] NAMES = getEnumNames(Color.class);
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
