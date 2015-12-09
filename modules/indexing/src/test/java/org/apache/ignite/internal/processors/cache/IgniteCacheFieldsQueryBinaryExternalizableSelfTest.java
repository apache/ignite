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
package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheFieldsQueryBinaryExternalizableSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(ExternalizableObject.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());

        entity.setFields(fields);

        cache.setQueryEntities(Arrays.asList(entity));

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuery() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(null);

        Map<Integer, String> putVals = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            cache.put(i, new ExternalizableObject(i, "object-" + i));

            if (i < 50)
                putVals.put(i, "object-" + i);
        }

        QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select id, name from ExternalizableObject " +
            "where id < ?").setArgs(50));

        List<List<?>> vals = cur.getAll();

        assertEquals(50, vals.size());

        for (List<?> val : vals) {
            Integer id = (Integer)val.get(0);
            String name = (String)val.get(1);

            assertEquals(putVals.get(id), name);
        }
    }

    /**
     *
     */
    private static class ExternalizableObject implements Externalizable {
        /** ID. */
        @QuerySqlField
        private int id;

        /** Name. */
        @QuerySqlField(index = false)
        private String name;

        /**
         * Empty constructor required for {@link java.io.Externalizable}.
         */
        public ExternalizableObject() {
            // No-op.
        }

        /**
         * @param id Id.
         * @param name Name.
         */
        private ExternalizableObject(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(id);
            out.writeUTF(name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = in.readInt();
            name = in.readUTF();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ExternalizableObject.class, this);
        }
    }
}
