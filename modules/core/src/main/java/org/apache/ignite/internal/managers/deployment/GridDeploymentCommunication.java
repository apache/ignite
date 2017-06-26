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

package org.apache.ignite.internal.managers.deployment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteNotPeerDeployable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CLASSLOAD;

/**
 * Communication helper class. Provides request and response sending methods.
 * It uses communication manager as a way of sending and receiving requests.
 */
@GridToStringExclude
class GridDeploymentCommunication {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridMessageListener peerLsnr;

    /** */
    private final ThreadLocal<Collection<UUID>> activeReqNodeIds = new ThreadLocal<>();

    /** */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** */
    private final Marshaller marsh;

    /**
     * Creates new instance of deployment communication.
     *
     * @param ctx Kernal context.
     * @param log Logger.
     */
    GridDeploymentCommunication(final GridKernalContext ctx, IgniteLogger log) {
        assert log != null;

        this.ctx = ctx;
        this.log = log.getLogger(getClass());

        peerLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                processDeploymentRequest(nodeId, msg);
            }
        };

        marsh = ctx.config().getMarshaller();
    }

    /**
     * Starts deployment communication.
     */
    void start() {
        ctx.io().addMessageListener(TOPIC_CLASSLOAD, peerLsnr);

        if (log.isDebugEnabled())
            log.debug("Started deployment communication.");
    }

    /**
     * Stops deployment communication.
     */
    void stop() {
        if (log.isDebugEnabled())
            log.debug("Stopping deployment communication.");

        busyLock.block();

        ctx.io().removeMessageListener(TOPIC_CLASSLOAD, peerLsnr);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Request.
     */
    private void processDeploymentRequest(UUID nodeId, Object msg) {
        assert nodeId != null;
        assert msg != null;

        if (!busyLock.enterBusy()) {
            if (log.isDebugEnabled())
                log.debug("Ignoring deployment request since grid is stopping " +
                    "[nodeId=" + nodeId + ", msg=" + msg + ']');

            return;
        }

        try {
            GridDeploymentRequest req = (GridDeploymentRequest)msg;

            if (req.isUndeploy())
                processUndeployRequest(nodeId, req);
            else {
                assert activeReqNodeIds.get() == null;

                Collection<UUID> nodeIds = req.nodeIds();

                nodeIds = nodeIds == null ? new HashSet<UUID>() : new HashSet<>(nodeIds);

                boolean b = nodeIds.add(nodeId);

                assert b;

                activeReqNodeIds.set(nodeIds);

                try {
                    processResourceRequest(nodeId, req);
                }
                finally {
                    activeReqNodeIds.set(null);
                }
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Undeploy request.
     */
    private void processUndeployRequest(UUID nodeId, GridDeploymentRequest req) {
        if (log.isDebugEnabled())
            log.debug("Received undeploy request [nodeId=" + nodeId + ", req=" + req + ']');

        ctx.deploy().undeployTask(nodeId, req.resourceName());
    }

    /**
     * Handles classes/resources requests.
     *
     * @param nodeId Originating node id.
     * @param req Request.
     */
    private void processResourceRequest(UUID nodeId, GridDeploymentRequest req) {
        if (log.isDebugEnabled())
            log.debug("Received peer class/resource loading request [node=" + nodeId + ", req=" + req + ']');

        if (req.responseTopic() == null) {
            try {
                req.responseTopic(U.unmarshal(marsh, req.responseTopicBytes(), U.resolveClassLoader(ctx.config())));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to process deployment request (will ignore): " + req, e);

                return;
            }
        }

        GridDeploymentResponse res = new GridDeploymentResponse();

        GridDeployment dep = ctx.deploy().getDeployment(req.classLoaderId());

        // Null class loader means failure here.
        if (dep != null) {
            ClassLoader ldr = dep.classLoader();

            // In case the class loader is ours - skip the check
            // since it was already performed before (and was successful).
            if (!(ldr instanceof GridDeploymentClassLoader)) {
                // First check for @GridNotPeerDeployable annotation.
                try {
                    String clsName = req.resourceName().replace('/', '.');

                    int idx = clsName.indexOf(".class");

                    if (idx >= 0)
                        clsName = clsName.substring(0, idx);

                    Class<?> cls = Class.forName(clsName, true, ldr);

                    if (U.getAnnotation(cls, IgniteNotPeerDeployable.class) != null) {
                        String errMsg = "Attempt to peer deploy class that has @GridNotPeerDeployable " +
                            "annotation: " + clsName;

                        U.error(log, errMsg);

                        res.errorMessage(errMsg);
                        res.success(false);

                        sendResponse(nodeId, req.responseTopic(), res);

                        return;
                    }
                }
                catch (ClassNotFoundException ignore) {
                    // Safely ignore it here - resource wasn't a class name.
                }
            }

            InputStream in = ldr.getResourceAsStream(req.resourceName());

            if (in == null) {
                String errMsg = "Requested resource not found (ignoring locally): " + req.resourceName();

                // Java requests the same class with BeanInfo suffix during
                // introspection automatically. Usually nobody uses this kind
                // of classes. Thus we print it out with DEBUG level.
                // Also we print it with DEBUG level because of the
                // frameworks which ask some classes just in case - for
                // example to identify whether certain framework is available.
                // Remote node will throw an exception if needs.
                if (log.isDebugEnabled())
                    log.debug(errMsg);

                res.success(false);
                res.errorMessage(errMsg);
            }
            else {
                try {
                    GridByteArrayList bytes = new GridByteArrayList(1024);

                    bytes.readAll(in);

                    res.success(true);
                    res.byteSource(bytes);
                }
                catch (IOException e) {
                    String errMsg = "Failed to read resource due to IO failure: " + req.resourceName();

                    U.error(log, errMsg, e);

                    res.errorMessage(errMsg);
                    res.success(false);
                }
                finally {
                    U.close(in, log);
                }
            }
        }
        else {
            String errMsg = "Failed to find local deployment for peer request: " + req;

            U.warn(log, errMsg);

            res.success(false);
            res.errorMessage(errMsg);
        }

        sendResponse(nodeId, req.responseTopic(), res);
    }

    /**
     * @param nodeId Destination node ID.
     * @param topic Response topic.
     * @param res Response.
     */
    private void sendResponse(UUID nodeId, Object topic, Message res) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node != null) {
            try {
                ctx.io().sendToCustomTopic(node, topic, res, GridIoPolicy.P2P_POOL);

                if (log.isDebugEnabled())
                    log.debug("Sent peer class loading response [node=" + node.id() + ", res=" + res + ']');
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send peer class loading response to node " +
                        "(node does not exist): " + nodeId);
            }
            catch (IgniteCheckedException e) {
                if (ctx.discovery().pingNodeNoError(nodeId))
                    U.error(log, "Failed to send peer class loading response to node: " + nodeId, e);
                else if (log.isDebugEnabled())
                    log.debug("Failed to send peer class loading response to node " +
                        "(node does not exist): " + nodeId);
            }
        }
        else if (log.isDebugEnabled())
                log.debug("Failed to send peer class loading response to node " +
                    "(node does not exist): " + nodeId);
    }


    /**
     * @param rsrcName Resource to undeploy.
     * @param rmtNodes Nodes to send request to.
     * @throws IgniteCheckedException If request could not be sent.
     */
    void sendUndeployRequest(String rsrcName, Collection<ClusterNode> rmtNodes) throws IgniteCheckedException {
        assert !rmtNodes.contains(ctx.discovery().localNode());

        Message req = new GridDeploymentRequest(null, null, rsrcName, true);

        if (!rmtNodes.isEmpty()) {
            ctx.io().sendToGridTopic(
                rmtNodes,
                TOPIC_CLASSLOAD,
                req,
                GridIoPolicy.P2P_POOL);
        }
    }

    /**
     * Sends request to the remote node and wait for response. If there is
     * no response until threshold time, method returns null.
     *
     *
     * @param rsrcName Resource name.
     * @param clsLdrId Class loader ID.
     * @param dstNode Remote node request should be sent to.
     * @param threshold Time in milliseconds when request is decided to
     *      be obsolete.
     * @return Either response value or {@code null} if timeout occurred.
     * @throws IgniteCheckedException Thrown if there is no connection with remote node.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    GridDeploymentResponse sendResourceRequest(final String rsrcName, IgniteUuid clsLdrId,
        final ClusterNode dstNode, long threshold) throws IgniteCheckedException {
        assert rsrcName != null;
        assert dstNode != null;
        assert clsLdrId != null;

        Collection<UUID> nodeIds = activeReqNodeIds.get();

        if (nodeIds != null && nodeIds.contains(dstNode.id())) {
            if (log.isDebugEnabled())
                log.debug("Node attempts to load resource from one of the requesters " +
                    "[rsrcName=" + rsrcName + ", dstNodeId=" + dstNode.id() +
                    ", requesters=" + nodeIds + ']');

            GridDeploymentResponse fake = new GridDeploymentResponse();

            fake.success(false);
            fake.errorMessage("Node attempts to load resource from one of the requesters " +
                "[rsrcName=" + rsrcName + ", dstNodeId=" + dstNode.id() +
                ", requesters=" + nodeIds + ']');

            return fake;
        }

        Object resTopic = TOPIC_CLASSLOAD.topic(IgniteUuid.fromUuid(ctx.localNodeId()));

        GridDeploymentRequest req = new GridDeploymentRequest(resTopic, clsLdrId, rsrcName, false);

        // Send node IDs chain with request.
        req.nodeIds(nodeIds);

        final Object qryMux = new Object();

        final GridTuple<GridDeploymentResponse> res = new GridTuple<>();

        GridLocalEventListener discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent;

                assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                UUID nodeId = discoEvt.eventNode().id();

                if (!nodeId.equals(dstNode.id()))
                    // Not a destination node.
                    return;

                GridDeploymentResponse fake = new GridDeploymentResponse();

                String errMsg = "Originating node left grid (resource will not be peer loaded) " +
                    "[nodeId=" + dstNode.id() + ", rsrc=" + rsrcName + ']';

                U.warn(log, errMsg);

                fake.success(false);
                fake.errorMessage(errMsg);

                // We put fake result here to interrupt waiting peer-to-peer thread
                // because originating node has left grid.
                synchronized (qryMux) {
                    res.set(fake);

                    qryMux.notifyAll();
                }
            }
        };

        GridMessageListener resLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert nodeId != null;
                assert msg != null;

                synchronized (qryMux) {
                    if (!(msg instanceof GridDeploymentResponse)) {
                        U.error(log, "Received unknown peer class loading response [node=" + nodeId + ", msg=" +
                            msg + ']');
                    }
                    else
                        res.set((GridDeploymentResponse)msg);

                    qryMux.notifyAll();
                }
            }
        };

        try {
            ctx.io().addMessageListener(resTopic, resLsnr);

            // The destination node has potentially left grid here but in this case
            // Communication manager will throw the exception while sending message.
            ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            long start = U.currentTimeMillis();

            if (req.responseTopic() != null && !ctx.localNodeId().equals(dstNode.id()))
                req.responseTopicBytes(U.marshal(marsh, req.responseTopic()));

            ctx.io().sendToGridTopic(dstNode, TOPIC_CLASSLOAD, req, GridIoPolicy.P2P_POOL);

            if (log.isDebugEnabled())
                log.debug("Sent peer class loading request [node=" + dstNode.id() + ", req=" + req + ']');

            synchronized (qryMux) {
                try {
                    long timeout = threshold - start;

                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for peer response from node [node=" + dstNode.id() +
                            ", timeout=" + timeout + ']');
                    }

                    while (res.get() == null && timeout > 0) {
                        qryMux.wait(timeout);

                        timeout = threshold - U.currentTimeMillis();
                    }
                }
                catch (InterruptedException e) {
                    // Interrupt again to get it in the users code.
                    Thread.currentThread().interrupt();

                    throw new IgniteCheckedException("Got interrupted while waiting for response from node: " +
                        dstNode.id(), e);
                }
            }

            if (res.get() == null) {
                U.warn(log, "Failed to receive peer response from node within duration [node=" + dstNode.id() +
                    ", duration=" + (U.currentTimeMillis() - start) + ']');
            }
            else if (log.isDebugEnabled())
                log.debug("Received peer loading response [node=" + dstNode.id() + ", res=" + res.get() + ']');

            return res.get();
        }
        finally {
            ctx.event().removeLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            ctx.io().removeMessageListener(resTopic, resLsnr);
        }
    }
}