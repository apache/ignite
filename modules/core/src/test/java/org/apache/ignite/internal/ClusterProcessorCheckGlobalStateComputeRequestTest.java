/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that the GridInternal annotation for GridClusterStateProcessor.CheckGlobalStateComputeRequest works correctly.
 */
public class ClusterProcessorCheckGlobalStateComputeRequestTest extends GridCommonAbstractTest {

    public ClusterProcessorCheckGlobalStateComputeRequestTest() {
        super(false);
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        if(igniteInstanceName.equalsIgnoreCase("daemon"))
            cfg.setDaemon(true);

        return cfg;
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckGlobalStateComputeRequest() throws Exception {
        startGrids(1);
        IgniteEx daemon = startGrid("daemon");

        //the GridInternal annotation will cause the job and response message to execute on management pool.
        for(int i = 0; i < 100; i++)
            daemon.cluster().active();
        checkBeanForAGrid(daemon, "Thread Pools", "GridManagementExecutor", "TaskCount", 100L);

        for(int i = 0; i < 100; i++)
            grid(0).cluster().active();
        checkBeanForAGrid(grid(0), "Thread Pools", "GridManagementExecutor", "TaskCount", 100L);
    }


    /** Checks that a bean with the specified group and name is available and has the expected attribute */
    private void checkBeanForAGrid(Ignite ignite, String grp, String name, String attributeName, Object expAttributeVal) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(ignite.name(), grp, name);
        Object attributeVal = ignite.configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        assertEquals(expAttributeVal, attributeVal);
    }


}
