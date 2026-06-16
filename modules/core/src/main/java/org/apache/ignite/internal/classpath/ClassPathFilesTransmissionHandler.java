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

package org.apache.ignite.internal.classpath;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager.TransmissionSender;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.classpath.ClassPathProcessor.fromMetastorage;
import static org.apache.ignite.internal.classpath.IgniteClassPathState.NEW;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_NODE_STOPPING_ERR_MSG;

/**
 * This manager is responsible for requesting and handling files from a remote node.
 */
class ClassPathFilesTransmissionHandler implements TransmissionHandler, GridMessageListener {
    /** ClassPath topic to receive files from remote node. */
    static final Object FILES_TOPIC = GridTopic.TOPIC_CLASSLOAD.topic("rmt_files");

    /** Transmission parameter for {@link IgniteClassPath#id()}. */
    private static final String ICP_ID_PARAM = "icpId";

    /** Transmission parameter for {@link IgniteClassPath} file name. */
    private static final String NAME_PARAM = "name";

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** */
    private volatile DownloadClassPathTask active;

    /**
     * Queue of asynchronous tasks to execute.
     * Head of queue is taks that currently executing.
     */
    private final Queue<DownloadClassPathTask> queue = new ConcurrentLinkedDeque<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** {@code true} if the node is stopping. */
    private volatile boolean stopping;

    /** */
    public ClassPathFilesTransmissionHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(ClassPathFilesTransmissionHandler.class);
    }

    /**
     * Downloads {@link IgniteClassPath} files locally from the remote node specified by {@code rmtNodeId}.
     * @param rmtNodeId Remote node id.
     * @param icpId ClassPath id.
     * @return Future for download operation.
     */
    IgniteInternalFuture<Void> downloadLocally(UUID rmtNodeId, UUID icpId) {
        try {
            IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

            if (rmtNodeId.equals(ctx.localNodeId())) {
                log.info("Skip download ClassPath files for upload node [name=" + icp.name() + ", id=" + icp.id() + ']');

                return new GridFinishedFuture<>();
            }

            log.info("Start download ClassPath files [name=" + icp.name() + ", id=" + icp.id() + ']');

            DownloadClassPathTask task = new DownloadClassPathTask(rmtNodeId, icp);

            try {
                submit(task);
            }
            catch (Throwable t) {
                task.res.onDone(t);
            }

            return task.res;
        }
        catch (Throwable e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /** Starts handler. */
    synchronized void start() {
        ctx.event().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            UUID leftNodeId = evt.eventNode().id();

            if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                onNodeLeft(leftNodeId);
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctx.io().addMessageListener(FILES_TOPIC, this);
        ctx.io().addTransmissionHandler(FILES_TOPIC, this);
    }

    /** Stopping handler. */
    void stop() {
        synchronized (this) {
            if (discoLsnr != null)
                ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(FILES_TOPIC);
            ctx.io().removeTransmissionHandler(FILES_TOPIC);

            stopping = true;
        }

        cancelAll(new IgniteException(SNP_NODE_STOPPING_ERR_MSG), r -> true);
    }

    /**
     * @param nodeId A node left the cluster.
     */
    void onNodeLeft(UUID nodeId) {
        cancelAll(
            new ClusterTopologyCheckedException("The node from which a snapshot has been requested left the grid"),
            r -> r.rmtNodeId.equals(nodeId)
        );
    }

    /**
     * @param err Task result.
     * @param filter Filter to select tasks for cancel.
     */
    private synchronized void cancelAll(Throwable err, Predicate<DownloadClassPathTask> filter) {
        queue.forEach(r -> cancel(r, err, filter));

        cancel(active, err, filter);
    }

    /**
     * @param r Task to cancel.
     * @param err Result error.
     * @param filter Task filter.
     * @return {@code True} if task was canceled.
     */
    private synchronized boolean cancel(DownloadClassPathTask r, Throwable err, Predicate<DownloadClassPathTask> filter) {
        if (r == null || !filter.test(r))
            return false;

        return r.res.onDone(err);
    }

    /** {@inheritDoc} */
    @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
        throw new UnsupportedOperationException("Loading file by chunks is not supported: " + nodeId);
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg0, byte plc) {
        try {
            if (msg0 instanceof DownloadClassPathMessage msg) {

                IgniteClassPath icp = null;

                try {
                    icp = fromMetastorage(msg.icpId, NEW, ctx);

                    NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

                    File root = ft.classPathRoot(icp.name());

                    if (!root.exists())
                        throw new IgniteException("Classpath root not exists: " + root);

                    // TODO: make async execution.
                    try (TransmissionSender sndr = ctx.io().openTransmissionSender(nodeId, FILES_TOPIC)) {
                        for (String name : icp.files()) {
                            File f = new File(root, name);

                            if (!f.exists())
                                throw new IgniteException("Classpath file not exists: " + f);

                            sndr.send(f, Map.of(ICP_ID_PARAM, icp.id(), NAME_PARAM, name), TransmissionPolicy.FILE);
                        }
                    }
                }
                catch (Throwable t) {
                    U.error(log, "Error processing classpath file request [request=" + msg + ", nodeId=" + nodeId + ']', t);

                    if (icp != null) {
                        ctx.io().sendToCustomTopic(nodeId,
                            FILES_TOPIC,
                            new DownloadClassPathFailureMessage(icp, t.getMessage()),
                            SYSTEM_POOL
                        );
                    }
                }
            }
            else if (msg0 instanceof DownloadClassPathFailureMessage msg) {

                String errMsg = "File download cancelled. ClassPath operation stopped on the remote node. Error: " + msg.err;

                if (log.isDebugEnabled())
                    log.debug(errMsg);

                if (!cancel(active, new IgniteException(errMsg), t -> t.icp.id().equals(msg.icpId))) {
                    if (log.isInfoEnabled()) {
                        log.warning("A stale ClassPath failure message has been received. Will be ignored " +
                            "[fromNodeId=" + nodeId + ", icpId=" + msg.icpId + "]: " + msg.err);
                    }
                }
            }
        }
        catch (Throwable e) {
            U.error(log, "Processing snapshot request from remote node fails with an error", e);

            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @Override public void onEnd(UUID nodeId) {
        DownloadClassPathTask task = active;

        if (!ensureTask(nodeId, task))
            return;

        int filesLeft = task.filesLeft.get();

        if (filesLeft != 0) {
            String msg = "onEnd invoked, but more files left: " + filesLeft + ", completing download process with an error";

            log.warning(msg);

            task.res.onDone(new IllegalStateException(msg));
        }

        if (log.isInfoEnabled())
            log.info("Classpath files from remote node has been fully received [icp=" + task.icp.name() + ']');

        task.res.onDone((Void)null);
    }

    /** {@inheritDoc} */
    @Override public void onException(UUID nodeId, Throwable ex) {
        DownloadClassPathTask task = active;

        if (!ensureTask(nodeId, task))
            return;

        task.res.onDone(ex);
    }

    /** {@inheritDoc} */
    @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
        UUID icpId = (UUID)fileMeta.params().get(ICP_ID_PARAM);
        String name = (String)fileMeta.params().get(NAME_PARAM);

        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        DownloadClassPathTask task = active;

        if (!ensureTask(nodeId, task)) {
            throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                "[icpId=" + icp.id() + ", file=" + name + ']');
        }

        NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

        File root = ft.classPathRoot(icp.name());

        NodeFileTree.mkdir(root, "classpath root");

        return new File(root, name).getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
        UUID icpId = (UUID)initMeta.params().get(ICP_ID_PARAM);
        String name = (String)initMeta.params().get(NAME_PARAM);

        IgniteClassPath icp = fromMetastorage(icpId, NEW, ctx);

        return file -> {
            DownloadClassPathTask task = active;

            if (!ensureTask(nodeId, task)) {
                throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                    "[icpId=" + icp.id() + ", file=" + name + ']');
            }

            int filesLeft = task.filesLeft.decrementAndGet();

            if (log.isInfoEnabled()) {
                log.info("Classpath file from remote node has been received " +
                    "[icp=" + task.icp.name() + ", file=" + name + ", filesLeft=" + filesLeft + ']');
            }
        };
    }

    /**
     * Starts {@code task} or adds it to queue.
     *
     * @param next Task to execute.
     */
    private void submit(DownloadClassPathTask next) {
        ClusterNode rmtNode;

        synchronized (this) {
            if (stopping) {
                next.res.onDone(new IgniteException(SNP_NODE_STOPPING_ERR_MSG));

                return;
            }

            if (active != null && !active.res.isDone()) {
                if (!queue.offer(next)) {
                    next.res.onDone(new IgniteException("Can't put task in queue: " + next.icp));
                }

                return;
            }

            rmtNode = ctx.discovery().node(next.rmtNodeId);

            if (rmtNode == null) {
                next.res.onDone(new IgniteException("Can't download classpath files. " +
                    "Remote node left the grid [rmtNodeId=" + rmtNode + ']'));

                return;
            }

            active = next;

            next.res.listen(this::onActiveDone);
        }

        try {
            // submit can be invoked from discovery thread.
            // sendOrderedMessage can be blocking so invok it in separate thread to release discovery.
            ctx.pools().getPeerClassLoadingExecutorService().submit(() -> {
                try {
                    ctx.cache().context().gridIO().sendOrderedMessage(
                        rmtNode,
                        FILES_TOPIC,
                        new DownloadClassPathMessage(next.icp),
                        SYSTEM_POOL,
                        Long.MAX_VALUE,
                        true
                    );
                }
                catch (IgniteCheckedException e) {
                    log.warning("Can't start download ClassPath files:", e);

                    next.res.onDone(new IgniteException("Can't download classpath files. " +
                        "Remote node left the grid [rmtNodeId=" + next.rmtNodeId + ']'));

                }
            });
        }
        catch (RejectedExecutionException e) {
            log.warning("Submit to P2P pool rejected", e);

            next.res.onDone(e);
        }
    }

    /** Starts next task if exists. */
    private void onActiveDone(IgniteInternalFuture<?> doneFut) {
        DownloadClassPathTask next;

        synchronized (this) {
            if (active == null || doneFut != active.res)
                return;

            active = null;

            next = queue.poll();

            while (next != null && next.res.isDone())
                next = queue.poll();
        }

        if (next != null)
            submit(next);
    }

    /** */
    private static boolean ensureTask(UUID nodeId, DownloadClassPathTask task) {
        return task != null
            && !task.res.isDone()
            && task.rmtNodeId.equals(nodeId);
    }

    /**
     * Task responsible for downloading {@link IgniteClassPath} files from remote node.
     */
    private static class DownloadClassPathTask {
        /** Node to download files from. */
        final UUID rmtNodeId;

        /** ClassPath to download files for. */
        final IgniteClassPath icp;

        /** Result of download. */
        final GridFutureAdapter<Void> res;

        /** Files counter. */
        final AtomicInteger filesLeft;

        /** */
        public DownloadClassPathTask(UUID rmtNodeId, IgniteClassPath icp) {
            this.rmtNodeId = rmtNodeId;
            this.icp = icp;
            this.res = new GridFutureAdapter<>();
            this.filesLeft = new AtomicInteger(icp.files().length);
        }
    }
}
