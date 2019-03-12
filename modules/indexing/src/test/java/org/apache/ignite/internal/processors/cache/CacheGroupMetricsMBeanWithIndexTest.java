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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Cache group JMX metrics test.
 */
public class CacheGroupMetricsMBeanWithIndexTest extends CacheGroupMetricsMBeanTest {
    /**
     *
     */
    protected static final String GROUP_NAME = "group1";

    /**
     *
     */
    protected static final String CACHE_NAME = "cache1";

    /**
     *
     */
    protected static final String OBJECT_NAME = "MyObject";

    /**
     *
     */
    protected static final String KEY_NAME = "id";

    /**
     *
     */
    protected static final String COLUMN1_NAME = "col1";

    /**
     *
     */
    protected static final String COLUMN2_NAME = "col2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME)
            .setGroupName(GROUP_NAME)
            .setAtomicityMode(atomicityMode());

        QueryEntity queryEntity = new QueryEntity(Long.class.getCanonicalName(), OBJECT_NAME);

        queryEntity.setKeyFieldName(KEY_NAME);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put(KEY_NAME, Long.class.getCanonicalName());

        fields.put(COLUMN1_NAME, Integer.class.getCanonicalName());

        fields.put(COLUMN2_NAME, String.class.getCanonicalName());

        queryEntity.setFields(fields);

        ArrayList<QueryIndex> indexes = new ArrayList<>();

        indexes.add(new QueryIndex(COLUMN1_NAME));

        indexes.add(new QueryIndex(COLUMN2_NAME));

        queryEntity.setIndexes(indexes);

        cacheCfg.setQueryEntities(Collections.singletonList(queryEntity));

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                .setMetricsEnabled(true)
            ).setMetricsEnabled(true)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test number of partitions need to finished indexes rebuilding.
     */
    @Test
    public void testIndexRebuildCountPartitionsLeft() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 500_000; i++) {
            Long id = Long.valueOf(i);

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME)
                .setField(KEY_NAME, id)
                .setField(COLUMN1_NAME, i / 2)
                .setField(COLUMN2_NAME, "str" + Integer.toHexString(i));

            cache1.put(id, o.build());
        }

        ignite.cluster().active(false);

        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        Collection<File> indexBinFiles = FileUtils.listFiles(dir, FileFilterUtils.nameFileFilter("index.bin"), TrueFileFilter.TRUE);

        for (File indexBin : indexBinFiles)
            U.delete(indexBin);

        ignite.cluster().active(true);

        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, GROUP_NAME);

        boolean stepStop = false;

        int timeout = 30;

        while (!stepStop && timeout > 0) {
            stepStop = mxBean0Grp1.getIndexRebuildCountPartitionsLeft() > 0;

            Thread.sleep(1000);

            timeout--;
        }

        Assert.assertTrue("Timeout wait start rebuild index", stepStop);

        stepStop = false;

        timeout = 30;

        while (!stepStop && timeout > 0) {
            stepStop = mxBean0Grp1.getIndexRebuildCountPartitionsLeft() == 0;
            //TODO
            System.out.println("TODO mxBean0Grp1 " + mxBean0Grp1.getIndexRebuildCountPartitionsLeft());
            Thread.sleep(1000);

            timeout--;
        }

        Assert.assertTrue("Timeout wait finished rebuild index", stepStop);
    }
}
