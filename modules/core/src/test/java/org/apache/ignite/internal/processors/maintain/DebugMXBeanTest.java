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

package org.apache.ignite.internal.processors.maintain;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.DebugMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DebugMXBeanTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDebugPage() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        ignite.createCache(CACHE_NAME);
        try (IgniteDataStreamer<Integer, Integer> st = ignite.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < 10_000; i++)
                st.addData(i, i);
        }

        DebugMXBean debugBean = bltMxBean();


        List<FileWALPointer> wal = new ArrayList<>();

        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        long pageId = 0;
        try (WALIterator it = factory.iterator(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(workDir)
        )) {
            while (it.hasNext()) {
                WALRecord record = it.next().get2();

                if (record instanceof PageSnapshot) {
                    pageId = ((PageSnapshot)record).fullPageId().pageId();
                }
            }
        }



        debugBean.dumpPageHistory(true, true, null, pageId);

    }

    /**
     *
     */
    private DebugMXBean bltMxBean() throws Exception {
        ObjectName mBeanName = U.makeMBeanName(getTestIgniteInstanceName(), "Debug",
            DebugMXBeanImpl.class.getSimpleName());

        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        assertTrue(mBeanSrv.isRegistered(mBeanName));

        Class<DebugMXBean> itfCls = DebugMXBean.class;

        return MBeanServerInvocationHandler.newProxyInstance(mBeanSrv, mBeanName, itfCls, true);
    }
}
