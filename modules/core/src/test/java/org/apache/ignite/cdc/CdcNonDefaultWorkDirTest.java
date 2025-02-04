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

package org.apache.ignite.cdc;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/** */
public class CdcNonDefaultWorkDirTest extends GridCommonAbstractTest {
    /** */
    public static final String CONSISTENT_ID = "node";

    /** */
    public static final String DFLT_WORK_DIR;

    static {
        String dir = null;

        try {
            dir = U.defaultWorkDirectory();
        }
        catch (IgniteCheckedException ignored) {
            // No-op.
        }

        DFLT_WORK_DIR = dir;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        NodeFileTree ft = new NodeFileTree(new File(DFLT_WORK_DIR), CONSISTENT_ID);

        assertNotNull(DFLT_WORK_DIR);
        assertTrue(ft.db().exists() || ft.db().mkdirs());
        assertTrue(ft.walCdc().mkdirs());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.delete(new File(DFLT_WORK_DIR));
        U.nullifyHomeDirectory();
        U.setIgniteHome(DFLT_WORK_DIR);
    }

    /** Tests CDC start with non default work directory. */
    @Test
    public void testCdcStartWithNonDefaultWorkDir() throws Exception {
        U.nullifyHomeDirectory();

        U.setIgniteHome("/not/existed/ignite/home");

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setWorkDirectory(DFLT_WORK_DIR)
            .setDataStorageConfiguration(new DataStorageConfiguration())
            .setConsistentId(CONSISTENT_ID);

        cfg.getDataStorageConfiguration()
            .getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setCdcEnabled(true);

        CountDownLatch started = new CountDownLatch(1);

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(new AbstractCdcTest.UserCdcConsumer() {
            @Override public void start(MetricRegistry mreg) {
                super.start(mreg);

                started.countDown();
            }
        });

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new CdcMain(cfg, null, cdcCfg));

        try {
            assertTrue(started.await(10, SECONDS));
        }
        finally {
            fut.cancel();
        }
    }
}
