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

package org.apache.ignite.failure;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.worker.FailureHandlingMxBeanImpl;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.mxbean.FailureHandlingMxBean;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;

/**
 * Tests configuration parameters related to failure handling.
 */
public class FailureHandlingConfigurationTest extends GridCommonAbstractTest {
    /** */
    private Long checkpointReadLockTimeout;

    /** */
    private Long sysWorkerBlockedTimeout;

    /** */
    private CountDownLatch failureLatch;

    /** */
    private class TestFailureHandler extends AbstractFailureHandler {
        /** */
        TestFailureHandler() {
            failureLatch = new CountDownLatch(1);
        }

        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            failureLatch.countDown();

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new TestFailureHandler());

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        if (checkpointReadLockTimeout != null)
            dsCfg.setCheckpointReadLockTimeout(checkpointReadLockTimeout);

        cfg.setDataStorageConfiguration(dsCfg);

        if (sysWorkerBlockedTimeout != null)
            cfg.setSystemWorkerBlockedTimeout(sysWorkerBlockedTimeout);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        sysWorkerBlockedTimeout = null;
        checkpointReadLockTimeout = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCfgParamsPropagation() throws Exception {
        sysWorkerBlockedTimeout = 30_000L;
        checkpointReadLockTimeout = 20_000L;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        FailureHandlingMxBean mBean = getMBean();

        assertEquals(sysWorkerBlockedTimeout.longValue(), reg.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointReadLockTimeout.longValue(), dbMgr.checkpointReadLockTimeout());

        assertEquals(sysWorkerBlockedTimeout.longValue(), mBean.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointReadLockTimeout.longValue(), mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartialCfgParamsPropagation() throws Exception {
        sysWorkerBlockedTimeout = 30_000L;
        checkpointReadLockTimeout = null;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        FailureHandlingMxBean mBean = getMBean();

        assertEquals(sysWorkerBlockedTimeout.longValue(), reg.getSystemWorkerBlockedTimeout());
        assertEquals(sysWorkerBlockedTimeout.longValue(), dbMgr.checkpointReadLockTimeout());

        assertEquals(sysWorkerBlockedTimeout.longValue(), mBean.getSystemWorkerBlockedTimeout());
        assertEquals(sysWorkerBlockedTimeout.longValue(), mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeParamValues() throws Exception {
        sysWorkerBlockedTimeout = -1L;
        checkpointReadLockTimeout = -85L;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        FailureHandlingMxBean mBean = getMBean();

        assertEquals(0L, reg.getSystemWorkerBlockedTimeout());
        assertEquals(-85L, dbMgr.checkpointReadLockTimeout());

        assertEquals(0L, mBean.getSystemWorkerBlockedTimeout());
        assertEquals(-85L, mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT, value = "80000")
    @WithSystemProperty(key = IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT, value = "90000")
    public void testOverridingBySysProps() throws Exception {
        sysWorkerBlockedTimeout = 1L;
        checkpointReadLockTimeout = 2L;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        WorkersRegistry reg = ignite.context().workersRegistry();

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        FailureHandlingMxBean mBean = getMBean();

        assertEquals(sysWorkerBlockedTimeout, ignite.configuration().getSystemWorkerBlockedTimeout());
        assertEquals(checkpointReadLockTimeout,
            ignite.configuration().getDataStorageConfiguration().getCheckpointReadLockTimeout());

        long workerPropVal = Long.getLong(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT);
        long checkpointPropVal = Long.getLong(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT);

        assertEquals(workerPropVal, reg.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointPropVal, dbMgr.checkpointReadLockTimeout());

        assertEquals(workerPropVal, mBean.getSystemWorkerBlockedTimeout());
        assertEquals(checkpointPropVal, mBean.getCheckpointReadLockTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMBeanParamsChanging() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        FailureHandlingMxBean mBean = getMBean();

        mBean.setSystemWorkerBlockedTimeout(80_000L);
        assertEquals(80_000L, ignite.context().workersRegistry().getSystemWorkerBlockedTimeout());

        mBean.setCheckpointReadLockTimeout(90_000L);
        assertEquals(90_000L, ignite.context().cache().context().database().checkpointReadLockTimeout());

        assertTrue(mBean.getLivenessCheckEnabled());
        mBean.setLivenessCheckEnabled(false);
        assertFalse(ignite.context().workersRegistry().livenessCheckEnabled());
        ignite.context().workersRegistry().livenessCheckEnabled(true);
        assertTrue(mBean.getLivenessCheckEnabled());
    }

    /** */
    private FailureHandlingMxBean getMBean() throws Exception {
        return getMxBean(getTestIgniteInstanceName(0), "Kernal",
            FailureHandlingMxBeanImpl.class, FailureHandlingMxBean.class);
    }
}
