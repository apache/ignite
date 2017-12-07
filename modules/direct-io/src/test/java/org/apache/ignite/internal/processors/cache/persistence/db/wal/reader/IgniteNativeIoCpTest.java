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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import com.google.common.base.Strings;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.AlignedBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.AlignedBuffersDirectFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;

public class IgniteNativeIoCpTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setPageSize(4 * 1024);

        DataRegionConfiguration regCfg = new DataRegionConfiguration();
/*
        regCfg.setName("dfltDataRegion");
        regCfg.setInitialSize(1024 * 1024 * 1024);
        regCfg.setMaxSize(1024 * 1024 * 1024);*/
        regCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(regCfg);

        configuration.setDataStorageConfiguration(dsCfg);
        return configuration;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.deleteDbFiles();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    public void testRecoveryAfterCpEnd() throws Exception {
        IgniteEx ignite = startGrid(0);

        ConcurrentHashMap8<Long, String> map8 = setupDirect(ignite);

        ignite.active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("cache");

        for (int i = 0; i < 10000; i++)
            cache.put(i, valueForKey(i));

        ignite.context().cache().context().database().waitForCheckpoint("test");

        stopAllGrids();


        IgniteEx igniteRestart = startGrid(0);
        igniteRestart.active(true);

        IgniteCache<Object, Object> cacheRestart = igniteRestart.getOrCreateCache("cache");

        for (int i = 0; i < 1000; i++)
            assertEquals(valueForKey(i), cacheRestart.get(i));

        System.err.println("Buffers: " + map8.size());
        for (Map.Entry<Long, String> next : map8.entrySet()) {
            System.err.println(next);
        }
        stopAllGrids();
        for (Long next : map8.keySet()) {
            AlignedBuffer.free(next);
        }
        map8.clear();
        System.err.println("Buffers: " + map8);
    }

    private ConcurrentHashMap8<Long, String> setupDirect(IgniteEx ignite) {
        final FilePageStoreManager pageStore = (FilePageStoreManager)ignite.context().cache().context().pageStore();

        final AlignedBuffersDirectFileIOFactory factory = new AlignedBuffersDirectFileIOFactory(
            ignite.log(),
            pageStore.workDir(),
            pageStore.pageSize(),
            pageStore.getPageStoreFileIoFactory());
        final ConcurrentHashMap8<Long, String> buffers = factory.managedAlignedBuffers();
        pageStore.pageStoreFileIoFactory(factory);

        if(factory.isDirectIoAvailable()) {
            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

            db.setThreadBuf(new ThreadLocal<ByteBuffer>() {
                /** {@inheritDoc} */
                @Override protected ByteBuffer initialValue() {
                    return factory.createManagedBuffer(pageStore.pageSize());
                }
            });
        }

        return buffers;
    }

    /**
     * @param i key.
     * @return
     */
    @NotNull private String valueForKey(int i) {
        return Strings.repeat(Integer.toString(i), 10);
    }
}
