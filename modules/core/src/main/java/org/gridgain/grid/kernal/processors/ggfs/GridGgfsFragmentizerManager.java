/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileAffinityRange.*;

/**
 * GGFS fragmentizer manager.
 */
public class GridGgfsFragmentizerManager extends GridGgfsManager {
    /** Message offer wait interval. */
    private static final int MSG_OFFER_TIMEOUT = 1000;

    /** Fragmentizer files check interval. */
    private static final int FRAGMENTIZER_CHECK_INTERVAL = 3000;

    /** Message send retry interval. */
    private static final int MESSAGE_SEND_RETRY_INTERVAL = 1000;

    /** How many times retry message send. */
    private static final int MESSAGE_SEND_RETRY_COUNT = 3;

    /** Manager stopping flag. */
    private volatile boolean stopping;

    /** Coordinator worker. */
    private volatile FragmentizerCoordinator fragmentizerCrd;

    /** This variable is used in tests only. */
    @SuppressWarnings("FieldCanBeLocal")
    private volatile boolean fragmentizerEnabled = true;

    /** Fragmentizer worker. */
    private FragmentizerWorker fragmentizerWorker;

    /** Shutdown lock. */
    private GridSpinReadWriteLock rw = new GridSpinReadWriteLock();

    /** Message topic. */
    private Object topic;

    /** {@inheritDoc} */
    @Override protected void start0() throws GridException {
        if (!ggfsCtx.configuration().isFragmentizerEnabled())
            return;

        // We care only about node leave and fail events.
        ggfsCtx.kernalContext().event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

                checkLaunchCoordinator(discoEvt);
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        fragmentizerWorker = new FragmentizerWorker();

        String ggfsName = ggfsCtx.configuration().getName();

        topic = F.isEmpty(ggfsName) ? TOPIC_GGFS : TOPIC_GGFS.topic(ggfsName);

        ggfsCtx.kernalContext().io().addMessageListener(topic, fragmentizerWorker);

        new GridThread(fragmentizerWorker).start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        if (ggfsCtx.configuration().isFragmentizerEnabled()) {
            // Check at startup if this node is a fragmentizer coordinator.
            GridDiscoveryEvent locJoinEvt = ggfsCtx.kernalContext().discovery().localJoinEvent();

            checkLaunchCoordinator(locJoinEvt);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override protected void onKernalStop0(boolean cancel) {
        boolean interrupted = false;

        // Busy wait is intentional.
        while (true) {
            try {
                if (rw.tryWriteLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }
        }

        try {
            if (interrupted)
                Thread.currentThread().interrupt();

            stopping = true;
        }
        finally {
            rw.writeUnlock();
        }

        synchronized (this) {
            if (fragmentizerCrd != null)
                fragmentizerCrd.cancel();
        }

        if (fragmentizerWorker != null)
            fragmentizerWorker.cancel();

        U.join(fragmentizerCrd, log);
        U.join(fragmentizerWorker, log);
    }

    /**
     * @param nodeId Node ID to send message to.
     * @param msg Message to send.
     * @throws GridException If send failed.
     */
    private void sendWithRetries(UUID nodeId, GridGgfsCommunicationMessage msg) throws GridException {
        for (int i = 0; i < MESSAGE_SEND_RETRY_COUNT; i++) {
            try {
                ggfsCtx.send(nodeId, topic, msg, SYSTEM_POOL);

                return;
            }
            catch (GridException e) {
                if (!ggfsCtx.kernalContext().discovery().alive(nodeId))
                    throw new GridTopologyException("Failed to send message (node left the grid) " +
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');

                if (i == MESSAGE_SEND_RETRY_COUNT - 1)
                    throw e;

                U.sleep(MESSAGE_SEND_RETRY_INTERVAL);
            }
        }
    }

    /**
     * Checks if current node is the oldest node in topology and starts coordinator thread if so.
     * Note that once node is the oldest one, it will be the oldest until it leaves grid.
     *
     * @param discoEvt Discovery event.
     */
    private void checkLaunchCoordinator(GridDiscoveryEvent discoEvt) {
        rw.readLock();

        try {
            if (stopping)
                return;

            if (fragmentizerCrd == null) {
                long minNodeOrder = Long.MAX_VALUE;

                Collection<ClusterNode> nodes = discoEvt.topologyNodes();

                for (ClusterNode node : nodes) {
                    if (node.order() < minNodeOrder && ggfsCtx.ggfsNode(node))
                        minNodeOrder = node.order();
                }

                ClusterNode locNode = ggfsCtx.kernalContext().grid().localNode();

                if (locNode.order() == minNodeOrder) {
                    if (log.isDebugEnabled())
                        log.debug("Detected local node to be the eldest GGFS node in topology, starting fragmentizer " +
                            "coordinator thread [discoEvt=" + discoEvt + ", locNode=" + locNode + ']');

                    synchronized (this) {
                        if (fragmentizerCrd == null && !stopping) {
                            fragmentizerCrd = new FragmentizerCoordinator();

                            new GridThread(fragmentizerCrd).start();
                        }
                    }
                }
            }
        }
        finally {
            rw.readUnlock();
        }
    }

    /**
     * Processes fragmentizer request. For each range assigned to this node:
     * <ul>
     *     <li>Mark range as moving indicating that block copying started.</li>
     *     <li>Copy blocks to non-colocated keys.</li>
     *     <li>Update map to indicate that blocks were copied and old blocks should be deleted.</li>
     *     <li>Delete old blocks.</li>
     *     <li>Remove range from file map.</li>
     * </ul>
     *
     * @param req Request.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("fallthrough")
    private void processFragmentizerRequest(GridGgfsFragmentizerRequest req) throws GridException {
        req.finishUnmarshal(ggfsCtx.kernalContext().config().getMarshaller(), null);

        Collection<GridGgfsFileAffinityRange> ranges = req.fragmentRanges();
        GridUuid fileId = req.fileId();

        GridGgfsFileInfo fileInfo = ggfsCtx.meta().info(fileId);

        if (fileInfo == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find file info for fragmentizer request: " + req);

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Moving file ranges for fragmentizer request [req=" + req + ", fileInfo=" + fileInfo + ']');

        for (GridGgfsFileAffinityRange range : ranges) {
            try {
                GridGgfsFileInfo updated;

                switch (range.status()) {
                    case RANGE_STATUS_INITIAL: {
                        // Mark range as moving.
                        updated = ggfsCtx.meta().updateInfo(fileId, updateRange(range, RANGE_STATUS_MOVING));

                        if (updated == null) {
                            ggfsCtx.data().cleanBlocks(fileInfo, range, true);

                            continue;
                        }

                        // Fall-through.
                    }

                    case RANGE_STATUS_MOVING: {
                        // Move colocated blocks.
                        ggfsCtx.data().spreadBlocks(fileInfo, range);

                        // Mark range as moved.
                        updated = ggfsCtx.meta().updateInfo(fileId, updateRange(range, RANGE_STATUS_MOVED));

                        if (updated == null) {
                            ggfsCtx.data().cleanBlocks(fileInfo, range, true);

                            continue;
                        }

                        // Fall-through.
                    }

                    case RANGE_STATUS_MOVED: {
                        // Remove old blocks.
                        ggfsCtx.data().cleanBlocks(fileInfo, range, false);

                        // Remove range from map.
                        updated = ggfsCtx.meta().updateInfo(fileId, deleteRange(range));

                        if (updated == null)
                            ggfsCtx.data().cleanBlocks(fileInfo, range, true);
                    }
                }
            }
            catch (GridGgfsInvalidRangeException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to update file range " +
                        "[range=" + range + "fileId=" + fileId + ", err=" + e.getMessage() + ']');
            }
        }
    }

    /**
     * Creates update info closure that will mark given range as moving.
     *
     * @param range Range to mark as moving.
     * @param status Status.
     * @return Update closure.
     */
    private GridClosure<GridGgfsFileInfo, GridGgfsFileInfo> updateRange(final GridGgfsFileAffinityRange range,
        final int status) {
        return new CX1<GridGgfsFileInfo, GridGgfsFileInfo>() {
            @Override public GridGgfsFileInfo applyx(GridGgfsFileInfo info) throws GridException {
                GridGgfsFileMap map = new GridGgfsFileMap(info.fileMap());

                map.updateRangeStatus(range, status);

                if (log.isDebugEnabled())
                    log.debug("Updated file map for range [fileId=" + info.id() + ", range=" + range +
                        ", status=" + status + ", oldMap=" + info.fileMap() + ", newMap=" + map + ']');

                GridGgfsFileInfo updated = new GridGgfsFileInfo(info, info.length());

                updated.fileMap(map);

                return updated;
            }
        };
    }

    /**
     * Creates update info closure that will mark given range as moving.
     *
     * @param range Range to mark as moving.
     * @return Update closure.
     */
    private GridClosure<GridGgfsFileInfo, GridGgfsFileInfo> deleteRange(final GridGgfsFileAffinityRange range) {
        return new CX1<GridGgfsFileInfo, GridGgfsFileInfo>() {
            @Override public GridGgfsFileInfo applyx(GridGgfsFileInfo info) throws GridException {
                GridGgfsFileMap map = new GridGgfsFileMap(info.fileMap());

                map.deleteRange(range);

                if (log.isDebugEnabled())
                    log.debug("Deleted range from file map [fileId=" + info.id() + ", range=" + range +
                        ", oldMap=" + info.fileMap() + ", newMap=" + map + ']');

                GridGgfsFileInfo updated = new GridGgfsFileInfo(info, info.length());

                updated.fileMap(map);

                return updated;
            }
        };
    }

    /**
     * Fragmentizer coordinator thread.
     */
    private class FragmentizerCoordinator extends GridWorker implements GridLocalEventListener, GridMessageListener {
        /** Files being fragmented. */
        private ConcurrentMap<GridUuid, Collection<UUID>> fragmentingFiles = new ConcurrentHashMap<>();

        /** Node IDs captured on start. */
        private volatile Collection<UUID> startSync;

        /** Wait lock. */
        private Lock lock = new ReentrantLock();

        /** Wait condition. */
        private Condition cond = lock.newCondition();

        /**
         * Constructor.
         */
        protected FragmentizerCoordinator() {
            super(ggfsCtx.kernalContext().gridName(), "fragmentizer-coordinator", ggfsCtx.kernalContext().log());

            ggfsCtx.kernalContext().event().addLocalEventListener(this, EVT_NODE_LEFT, EVT_NODE_FAILED);
            ggfsCtx.kernalContext().io().addMessageListener(topic, this);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            // Wait for all previous fragmentizer tasks to complete.
            syncStart();

            while (!isCancelled()) {
                // If we have room for files, add them to fragmentizer.
                try {
                    while (fragmentingFiles.size() < ggfsCtx.configuration().getFragmentizerConcurrentFiles()) {
                        GridGgfsFileInfo fileInfo = fileForFragmentizer(fragmentingFiles.keySet());

                        // If no colocated files found, exit loop.
                        if (fileInfo == null)
                            break;

                        requestFragmenting(fileInfo);
                    }
                }
                catch (GridException | GridRuntimeException e) {
                    if (!X.hasCause(e, InterruptedException.class) && !X.hasCause(e, GridInterruptedException.class))
                        LT.warn(log, e, "Failed to get fragmentizer file info (will retry).");
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Got interrupted exception in fragmentizer coordinator (grid is stopping).");

                        break; // While.
                    }
                }

                lock.lock();

                try {
                    cond.await(FRAGMENTIZER_CHECK_INTERVAL, MILLISECONDS);
                }
                finally {
                    lock.unlock();
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onEvent(GridEvent evt) {
            assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

            if (log.isDebugEnabled())
                log.debug("Processing node leave event: " + discoEvt);

            boolean signal = false;

            Collection<UUID> startSync0 = startSync;

            if (startSync0 != null && !startSync0.isEmpty()) {
                startSync0.remove(discoEvt.eventNode().id());

                if (startSync0.isEmpty()) {
                    if (log.isDebugEnabled())
                        log.debug("Completed fragmentizer coordinator sync start.");

                    signal = true;
                }
            }

            if (!signal) {
                Iterator<Map.Entry<GridUuid, Collection<UUID>>> it = fragmentingFiles.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<GridUuid, Collection<UUID>> entry = it.next();

                    Collection<UUID> nodeIds = entry.getValue();

                    if (nodeIds.remove(discoEvt.eventNode().id())) {
                        if (nodeIds.isEmpty()) {
                            if (log.isDebugEnabled())
                                log.debug("Received all responses for fragmentizer task [fileId=" + entry.getKey() +
                                    ']');

                            it.remove();

                            signal = true;
                        }
                    }
                }
            }

            if (signal)
                wakeUp();
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (msg instanceof GridGgfsFragmentizerResponse) {
                GridGgfsFragmentizerResponse res = (GridGgfsFragmentizerResponse)msg;

                GridUuid fileId = res.fileId();

                Collection<UUID> nodeIds = fragmentingFiles.get(fileId);

                if (nodeIds != null) {
                    if (nodeIds.remove(nodeId)) {
                        if (nodeIds.isEmpty()) {
                            if (log.isDebugEnabled())
                                log.debug("Received all responses for fragmentizer task [fileId=" + fileId + ']');

                            fragmentingFiles.remove(fileId, nodeIds);

                            wakeUp();
                        }
                    }
                }
                else
                    log.warning("Received fragmentizer response for file ID which was not requested (will ignore) " +
                        "[nodeId=" + nodeId + ", fileId=" + res.fileId() + ']');
            }
            else if (msg instanceof GridGgfsSyncMessage) {
                GridGgfsSyncMessage sync = (GridGgfsSyncMessage)msg;

                if (sync.response() && sync.order() == ggfsCtx.kernalContext().grid().localNode().order()) {
                    if (log.isDebugEnabled())
                        log.debug("Received fragmentizer sync response from remote node: " + nodeId);

                    Collection<UUID> startSync0 = startSync;

                    if (startSync0 != null) {
                        startSync0.remove(nodeId);

                        if (startSync0.isEmpty()) {
                            if (log.isDebugEnabled())
                                log.debug("Completed fragmentizer coordinator sync start: " + startSync0);

                            wakeUp();
                        }
                    }
                }
            }
        }

        /**
         * Signals condition.
         */
        private void wakeUp() {
            lock.lock();

            try {
                cond.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Sends sync message to remote nodes and awaits for response from all nodes.
         *
         * @throws InterruptedException If waiting was interrupted.
         */
        private void syncStart() throws InterruptedException {
            Collection<UUID> startSync0 = startSync = new GridConcurrentHashSet<>(
                F.viewReadOnly(
                    ggfsCtx.kernalContext().discovery().allNodes(),
                    F.node2id(),
                    new P1<ClusterNode>() {
                        @Override public boolean apply(ClusterNode n) {
                            return ggfsCtx.ggfsNode(n);
                        }
                    }));

            ClusterNode locNode = ggfsCtx.kernalContext().grid().localNode();

            while (!startSync0.isEmpty()) {
                for (UUID nodeId : startSync0) {
                    GridGgfsSyncMessage syncReq = new GridGgfsSyncMessage(locNode.order(), false);

                    try {
                        if (log.isDebugEnabled())
                            log.debug("Sending fragmentizer sync start request to remote node [nodeId=" + nodeId +
                                ", syncReq=" + syncReq + ']');

                        sendWithRetries(nodeId, syncReq);

                        // Close window between message sending and discovery event.
                        if (!ggfsCtx.kernalContext().discovery().alive(nodeId))
                            startSync0.remove(nodeId);
                    }
                    catch (GridException e) {
                        if (e.hasCause(GridTopologyException.class)) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send sync message to remote node (node has left the grid): " +
                                    nodeId);
                        }
                        else
                            U.error(log, "Failed to send synchronize message to remote node (will not wait for reply): " +
                                nodeId, e);

                        startSync0.remove(nodeId);
                    }
                }

                lock.lock();

                try {
                    if (!startSync0.isEmpty())
                        cond.await(10000, MILLISECONDS);
                }
                finally {
                    lock.unlock();
                }
            }
        }

        /**
         * Starts file fragmenting. Will group file affinity ranges by nodes and send requests to each node.
         * File will be considered processed when each node replied with success (or error) or left the grid.
         *
         * @param fileInfo File info to process.
         */
        private void requestFragmenting(GridGgfsFileInfo fileInfo) {
            GridGgfsFileMap map = fileInfo.fileMap();

            assert map != null && !map.ranges().isEmpty();

            Map<UUID, Collection<GridGgfsFileAffinityRange>> grpMap = U.newHashMap(map.ranges().size());

            for (GridGgfsFileAffinityRange range : map.ranges()) {
                UUID nodeId = ggfsCtx.data().affinityNode(range.affinityKey()).id();

                Collection<GridGgfsFileAffinityRange> nodeRanges = grpMap.get(nodeId);

                if (nodeRanges == null) {
                    nodeRanges = new LinkedList<>();

                    grpMap.put(nodeId, nodeRanges);
                }

                nodeRanges.addAll(range.split(ggfsCtx.data().groupBlockSize()));
            }

            Collection<UUID> nodeIds = new IdentityHashSet(grpMap.keySet());

            if (log.isDebugEnabled())
                log.debug("Calculating fragmentizer groups for file [fileInfo=" + fileInfo +
                    ", nodeIds=" + nodeIds + ']');

            // Put assignment to map first.
            Object old = fragmentingFiles.putIfAbsent(fileInfo.id(), nodeIds);

            assert old == null;

            for (Map.Entry<UUID, Collection<GridGgfsFileAffinityRange>> entry : grpMap.entrySet()) {
                UUID nodeId = entry.getKey();

                GridGgfsFragmentizerRequest msg = new GridGgfsFragmentizerRequest(fileInfo.id(), entry.getValue());

                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending fragmentizer request to remote node [nodeId=" + nodeId +
                            ", fileId=" + fileInfo.id() + ", msg=" + msg + ']');

                    sendWithRetries(nodeId, msg);
                }
                catch (GridException e) {
                    if (e.hasCause(GridTopologyException.class)) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send fragmentizer request to remote node (node left grid): " +
                                nodeId);
                    }
                    else
                        U.error(log, "Failed to send fragmentizer request to remote node [nodeId=" + nodeId +
                            ", msg=" + msg + ']', e);

                    nodeIds.remove(nodeId);
                }
            }

            if (nodeIds.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Got empty wait set for fragmentized file: " + fileInfo);

                fragmentingFiles.remove(fileInfo.id(), nodeIds);
            }
        }
    }

    /**
     * Gets next file for fragmentizer to be processed.
     *
     * @param exclude File IDs to exclude (the ones that are currently being processed).
     * @return File ID to process or {@code null} if there are no such files.
     * @throws GridException In case of error.
     */
    @Nullable private GridGgfsFileInfo fileForFragmentizer(Collection<GridUuid> exclude) throws GridException {
        return fragmentizerEnabled ? ggfsCtx.meta().fileForFragmentizer(exclude) : null;
    }

    /**
     * Fragmentizer worker thread.
     */
    private class FragmentizerWorker extends GridWorker implements GridMessageListener {
        /** Requests for this worker. */
        private BlockingQueue<GridBiTuple<UUID, GridGgfsCommunicationMessage>> msgs = new LinkedBlockingDeque<>();

        /**
         * Constructor.
         */
        protected FragmentizerWorker() {
            super(ggfsCtx.kernalContext().gridName(), "fragmentizer-worker", ggfsCtx.kernalContext().log());
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (msg instanceof GridGgfsFragmentizerRequest ||
                msg instanceof GridGgfsSyncMessage) {
                if (log.isDebugEnabled())
                    log.debug("Received fragmentizer request from remote node [nodeId=" + nodeId +
                        ", msg=" + msg + ']');

                GridBiTuple<UUID, GridGgfsCommunicationMessage> tup = F.t(nodeId, (GridGgfsCommunicationMessage)msg);

                try {
                    if (!msgs.offer(tup, MSG_OFFER_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        U.error(log, "Failed to process fragmentizer communication message (will discard) " +
                            "[nodeId=" + nodeId + ", msg=" + msg + ']');
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();

                    U.warn(log, "Failed to process fragmentizer communication message (thread was interrupted) "+
                        "[nodeId=" + nodeId + ", msg=" + msg + ']');
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            while (!isCancelled()) {
                GridBiTuple<UUID, GridGgfsCommunicationMessage> req = msgs.take();

                UUID nodeId = req.get1();

                if (req.get2() instanceof GridGgfsFragmentizerRequest) {
                    GridGgfsFragmentizerRequest fragmentizerReq = (GridGgfsFragmentizerRequest)req.get2();

                    if (!rw.tryReadLock()) {
                        if (log.isDebugEnabled())
                            log.debug("Received fragmentizing request while stopping grid (will ignore) " +
                                "[nodeId=" + nodeId + ", req=" + req.get2() + ']');

                        continue; // while.
                    }

                    try {
                        try {
                            processFragmentizerRequest(fragmentizerReq);
                        }
                        catch (GridException e) {
                            if (e.hasCause(GridTopologyException.class)) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to process fragmentizer request (remote node left the grid) " +
                                        "[req=" + req + ", err=" + e.getMessage() + ']');
                            }
                            else
                                U.error(log, "Failed to process fragmentizer request [nodeId=" + nodeId +
                                    ", req=" + req + ']', e);
                        }
                        finally {
                            sendResponse(nodeId, new GridGgfsFragmentizerResponse(fragmentizerReq.fileId()));
                        }
                    }
                    finally {
                        rw.readUnlock();
                    }
                }
                else {
                    assert req.get2() instanceof GridGgfsSyncMessage;

                    GridGgfsSyncMessage syncMsg = (GridGgfsSyncMessage)req.get2();

                    if (!syncMsg.response()) {
                        GridGgfsSyncMessage res = new GridGgfsSyncMessage(syncMsg.order(), true);

                        if (log.isDebugEnabled())
                            log.debug("Sending fragmentizer sync response to remote node [nodeId=" + nodeId +
                                ", res=" + res + ']');

                        sendResponse(nodeId, res);
                    }
                }
            }
        }

        /**
         * Sends response to remote node.
         *
         * @param nodeId Node ID to send response to.
         * @param msg Message to send.
         */
        private void sendResponse(UUID nodeId, GridGgfsCommunicationMessage msg) {
            try {
                sendWithRetries(nodeId, msg);
            }
            catch (GridException e) {
                if (e.hasCause(GridTopologyException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send sync response to GGFS fragmentizer coordinator " +
                            "(originating node left the grid): " + nodeId);
                }
                else
                    U.error(log, "Failed to send sync response to GGFS fragmentizer coordinator: " + nodeId, e);
            }
        }
    }

    /**
     * Hash set that overrides equals to use identity comparison.
     */
    private static class IdentityHashSet extends GridConcurrentHashSet<UUID> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         *
         * @param c Collection to add.
         */
        private IdentityHashSet(Collection<UUID> c) {
            super(c);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            // Identity comparison.
            return this == o;
        }
    }
}
