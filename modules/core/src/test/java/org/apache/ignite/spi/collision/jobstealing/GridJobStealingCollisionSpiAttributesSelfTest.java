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

package org.apache.ignite.spi.collision.jobstealing;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.GridCollisionTestContext;
import org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SPI_CLASS;
import static org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi.WAIT_JOBS_THRESHOLD_NODE_ATTR;

/**
 * Job stealing attributes test.
 */
@GridSpiTest(spi = JobStealingCollisionSpi.class, group = "Collision SPI")
public class GridJobStealingCollisionSpiAttributesSelfTest extends GridSpiAbstractTest<JobStealingCollisionSpi> {
    /** */
    private static GridTestNode rmtNode;

    /** */
    public GridJobStealingCollisionSpiAttributesSelfTest() {
        super(true /*start spi*/);
    }

    /**
     * @return Wait jobs threshold.
     */
    @GridSpiTestConfig
    public int getWaitJobsThreshold() {
        return 0;
    }

    /**
     * @return Message expiration time.
     */
    @GridSpiTestConfig
    public long getMessageExpireTime() {
        return 1;
    }

    /**
     * @return Active jobs threshold.
     */
    @GridSpiTestConfig
    public int getActiveJobsThreshold() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        GridTestNode locNode = new GridTestNode(UUID.randomUUID());

        addSpiDependency(locNode);

        ctx.setLocalNode(locNode);

        return ctx;
    }

    /**
     * Adds Failover SPI attribute.
     *
     * @param node Node to add attribute to.
     * @throws Exception If failed.
     */
    private void addSpiDependency(GridTestNode node) throws Exception {
        node.addAttribute(U.spiAttribute(getSpi(), ATTR_SPI_CLASS), JobStealingFailoverSpi.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        rmtNode = new GridTestNode(UUID.randomUUID());

        addSpiDependency(rmtNode);

        rmtNode.setAttribute(U.spiAttribute(getSpi(), WAIT_JOBS_THRESHOLD_NODE_ATTR), getWaitJobsThreshold());

        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        metrics.setCurrentWaitingJobs(2);

        rmtNode.setMetrics(metrics);

        getSpiContext().addNode(rmtNode);

        getSpi().setStealingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        getSpiContext().failNode(rmtNode);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSameAttribute() throws Exception {
        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        GridTestNode rmtNode = (GridTestNode)getSpiContext().remoteNodes().iterator().next();

        rmtNode.setAttribute("useCollision", true);

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        // Set up the same attribute and value as for remote node.
        getSpi().setStealingAttributes(F.asMap("useCollision", true));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Cleanup
        rmtNode.removeAttribute("useCollision");

        // Set up the same attribute and value as for remote node.
        getSpi().setStealingAttributes(Collections.<String, Serializable>emptyMap());

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should be sent to remote node because it has the same
        // attributes.
        assert msg != null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testEmptyRemoteAttribute() throws Exception {
        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        // Set up the same attribute and value as for remote node.
        getSpi().setStealingAttributes(F.asMap("useCollision", true));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Set up the same attribute and value as for remote node.
        getSpi().setStealingAttributes(Collections.<String, Serializable>emptyMap());

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should not be sent to remote node at it does not have attribute
        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testEmptyLocalAttribute() throws Exception {
        // Collision SPI does not allow to send more than 1 message in a
        // certain period of time (see getMessageExpireTime() method).
        // Thus we have to wait for the message to be expired.
        Thread.sleep(50);

        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        GridTestNode rmtNode = (GridTestNode)F.first(getSpiContext().remoteNodes());

        rmtNode.setAttribute("useCollision", true);

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Cleanup.
        rmtNode.removeAttribute("useCollision");

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should be sent to remote node because it has the same
        // attributes.
        assert msg != null;
    }

   /**
    * @throws Exception If test failed.
    */
    public void testDiffAttribute() throws Exception {
       List<CollisionJobContext> waitCtxs = Collections.emptyList();

       Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

      GridTestNode rmtNode = (GridTestNode)F.first(getSpiContext().remoteNodes());

       rmtNode.setAttribute("useCollision1", true);

       getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

       // Set up the same attribute and value as for remote node.
       getSpi().setStealingAttributes(F.asMap("useCollision2", true));

       getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

       // Cleanup
       rmtNode.removeAttribute("useCollision1");

       // Set up the same attribute and value as for remote node.
       getSpi().setStealingAttributes(Collections.<String, Serializable>emptyMap());

       // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

       // Message should be sent to remote node because it has the same
       // attributes.
       assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBothEmptyAttribute() throws Exception {
        // Collision SPI does not allow to send more than 1 message in a
        // certain period of time (see getMessageExpireTime() method).
        // Thus we have to wait for the message to be expired.
        Thread.sleep(50);

        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should be sent to remote node because it has the same
        // attributes.
        assert msg != null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testIsStealingOff() throws Exception {
        // Collision SPI does not allow to send more than 1 message in a
        // certain period of time (see getMessageExpireTime() method).
        // Thus we have to wait for the message to be expired.
        Thread.sleep(50);

        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpi().setStealingEnabled(false);

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should not be sent to remote node because stealing is off
        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testIsStealingOn() throws Exception {
        // Collision SPI does not allow to send more than 1 message in a
        // certain period of time (see getMessageExpireTime() method).
        // Thus we have to wait for the message to be expired.
        Thread.sleep(50);

        List<CollisionJobContext> waitCtxs = Collections.emptyList();

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpi().setStealingEnabled(true);

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        // Message should not be sent to remote node because stealing is on
        assert msg != null;
    }
}