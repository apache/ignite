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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheQueryBuildValueTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(TestBuilderValue.class.getName());

        ArrayList<QueryIndex> idxs = new ArrayList<>();

        QueryIndex idx = new QueryIndex("iVal");
        idxs.add(idx);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("iVal", Integer.class.getName());

        entity.setFields(fields);

        entity.setIndexes(idxs);

        ccfg.setQueryEntities(Collections.singleton(entity));

        cfg.setCacheConfiguration(ccfg);

        BinaryConfiguration binaryCfg = new BinaryConfiguration();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();
        typeCfg.setTypeName(TestBuilderValue.class.getName());

        binaryCfg.setTypeConfigurations(Collections.singletonList(typeCfg));

        cfg.setBinaryConfiguration(binaryCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBuilderAndQuery() throws Exception {
        Ignite node = ignite(0);

        final IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

        IgniteBinary binary = node.binary();

        BinaryObjectBuilder builder = binary.builder(TestBuilderValue.class.getName());

        cache.put(0, builder.build());

        builder.setField("iVal", 1);

        cache.put(1, builder.build());

        List<Cache.Entry<Object, Object>> entries =
            cache.query(new SqlQuery<>(TestBuilderValue.class, "true")).getAll();

        assertEquals(2, entries.size());
    }

    /**
     *
     */
    static class TestBuilderValue implements Serializable {
        /** */
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        public TestBuilderValue(int iVal) {
            this.iVal = iVal;
        }
    }
}
