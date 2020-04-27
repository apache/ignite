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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks mbeans validity for miscelenious spis.
 */
public class GridMbeansMiscTest extends GridCommonAbstractTest {
    /** */
    private CollisionSpi collisionSpi;

    /** */
    private LoadBalancingSpi loadBalancingSpi;

    /** */
    private DeploymentSpi deploymentSpi;

    /** */
    private CheckpointSpi checkpointSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        if (collisionSpi != null)
            cfg.setCollisionSpi(collisionSpi);

        if (loadBalancingSpi != null)
            cfg.setLoadBalancingSpi(loadBalancingSpi);

        if (deploymentSpi != null)
            cfg.setDeploymentSpi(deploymentSpi);

        if (checkpointSpi != null)
            cfg.setCheckpointSpi(checkpointSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMbeansSet1() throws Exception {
        collisionSpi = new FifoQueueCollisionSpi();
        loadBalancingSpi = new WeightedRandomLoadBalancingSpi();
        deploymentSpi = new LocalDeploymentSpi();
        checkpointSpi = new SharedFsCheckpointSpi();

        String[] beansToValidate = new String[] {
            "org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi$FifoQueueCollisionSpiMBeanImpl",
            "org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi$WeightedRandomLoadBalancingSpiMBeanImpl",
            "org.apache.ignite.spi.deployment.local.LocalDeploymentSpi$LocalDeploymentSpiMBeanImpl",
            "org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi$SharedFsCheckpointSpiMBeanImpl"
        };

        doTest(beansToValidate);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMbeansSet2() throws Exception {
        collisionSpi = new PriorityQueueCollisionSpi();
        loadBalancingSpi = new AdaptiveLoadBalancingSpi();

        String[] beansToValidate = new String[] {
            "org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi$PriorityQueueCollisionSpiMBeanImpl",
            "org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi$AdaptiveLoadBalancingSpiMBeanImpl"
        };

        doTest(beansToValidate);
    }

    /** */
    private void doTest(String[] beansToValidate) throws Exception {
        try {
            Ignite ignite = startGrid();

            validateMbeans(ignite, beansToValidate);
        }
        finally {
            stopAllGrids();
        }
    }
}
