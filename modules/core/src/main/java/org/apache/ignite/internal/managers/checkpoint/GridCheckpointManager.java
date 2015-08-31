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

package org.apache.ignite.internal.managers.checkpoint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.events.CheckpointEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.GridTaskSessionInternal;
import org.apache.ignite.internal.SkipDaemon;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.checkpoint.CheckpointListener;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_SAVED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CHECKPOINT;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 * This class defines a checkpoint manager.
 */
@SkipDaemon
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "deprecation"})
public class GridCheckpointManager extends GridManagerAdapter<CheckpointSpi> {
    /** Max closed topics to store. */
    public static final int MAX_CLOSED_SESS = 10240;

    /** */
    private final GridMessageListener lsnr = new CheckpointRequestListener();

    /** */
    private final ConcurrentMap<IgniteUuid, CheckpointSet> keyMap;

    /** */
    private final Collection<IgniteUuid> closedSess;

    /** Grid marshaller. */
    private final Marshaller marsh;

    /**
     * @param ctx Grid kernal context.
     */
    public GridCheckpointManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getCheckpointSpi());

        marsh = ctx.config().getMarshaller();

        if (enabled()) {
            keyMap = new ConcurrentHashMap8<>();

            closedSess = new GridBoundedConcurrentLinkedHashSet<>(MAX_CLOSED_SESS,
                MAX_CLOSED_SESS,
                0.75f,
                256,
                PER_SEGMENT_Q);
        }
        else {
            keyMap = null;

            closedSess = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (CheckpointSpi spi : getSpis()) {
            spi.setCheckpointListener(new CheckpointListener() {
                @Override public void onCheckpointRemoved(String key) {
                    record(EVT_CHECKPOINT_REMOVED, key);
                }
            });
        }

        startSpi();

        ctx.io().addMessageListener(TOPIC_CHECKPOINT, lsnr);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        GridIoManager comm = ctx.io();

        if (comm != null)
            comm.removeMessageListener(TOPIC_CHECKPOINT, lsnr);

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @return Session IDs.
     */
    public Collection<IgniteUuid> sessionIds() {
        return enabled() ? new ArrayList<>(keyMap.keySet()) : Collections.<IgniteUuid>emptyList();
    }

    /**
     * @param ses Task session.
     * @param key Checkpoint key.
     * @param state Checkpoint state to save.
     * @param scope Checkpoint scope.
     * @param timeout Checkpoint timeout.
     * @param override Whether or not override checkpoint if it already exists.
     * @return {@code true} if checkpoint has been actually saved, {@code false} otherwise.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public boolean storeCheckpoint(GridTaskSessionInternal ses,
        String key,
        Object state,
        ComputeTaskSessionScope scope,
        long timeout,
        boolean override)
        throws IgniteCheckedException
    {
        if (!enabled())
            return false;

        assert ses != null;
        assert key != null;

        long now = U.currentTimeMillis();

        boolean saved = false;

        try {
            switch (scope) {
                case GLOBAL_SCOPE: {
                    byte[] data = state == null ? null : marsh.marshal(state);

                    saved = getSpi(ses.getCheckpointSpi()).saveCheckpoint(key, data, timeout, override);

                    if (saved)
                        record(EVT_CHECKPOINT_SAVED, key);

                    break;
                }

                case SESSION_SCOPE: {
                    if (closedSess.contains(ses.getId())) {
                        U.warn(log, "Checkpoint will not be saved due to session invalidation [key=" + key +
                            ", val=" + state + ", ses=" + ses + ']',
                            "Checkpoint will not be saved due to session invalidation.");

                        break;
                    }

                    if (now > ses.getEndTime()) {
                        U.warn(log, "Checkpoint will not be saved due to session timeout [key=" + key +
                            ", val=" + state + ", ses=" + ses + ']',
                            "Checkpoint will not be saved due to session timeout.");

                        break;
                    }

                    if (now + timeout > ses.getEndTime() || now + timeout < 0)
                        timeout = ses.getEndTime() - now;

                    // Save it first to avoid getting null value on another node.
                    byte[] data = state == null ? null : marsh.marshal(state);

                    Set<String> keys = keyMap.get(ses.getId());

                    if (keys == null) {
                        Set<String> old = keyMap.putIfAbsent(ses.getId(),
                            (CheckpointSet)(keys = new CheckpointSet(ses.session())));

                        if (old != null)
                            keys = old;

                        // Double check.
                        if (closedSess.contains(ses.getId())) {
                            U.warn(log, "Checkpoint will not be saved due to session invalidation [key=" + key +
                                ", val=" + state + ", ses=" + ses + ']',
                                "Checkpoint will not be saved due to session invalidation.");

                            keyMap.remove(ses.getId(), keys);

                            break;
                        }
                    }

                    if (log.isDebugEnabled())
                        log.debug("Resolved keys for session [keys=" + keys + ", ses=" + ses +
                            ", keyMap=" + keyMap + ']');

                    // Note: Check that keys exists because session may be invalidated during saving
                    // checkpoint from GridFuture.
                    if (keys != null) {
                        // Notify master node.
                        if (ses.getJobId() != null) {
                            ClusterNode node = ctx.discovery().node(ses.getTaskNodeId());

                            if (node != null)
                                ctx.io().send(
                                    node,
                                    TOPIC_CHECKPOINT,
                                    new GridCheckpointRequest(ses.getId(), key, ses.getCheckpointSpi()),
                                    GridIoPolicy.PUBLIC_POOL);
                        }

                        saved = getSpi(ses.getCheckpointSpi()).saveCheckpoint(key, data, timeout, override);

                        if (saved) {
                            keys.add(key);

                            record(EVT_CHECKPOINT_SAVED, key);
                        }
                    }

                    break;
                }

                default:
                    assert false : "Unknown checkpoint scope: " + scope;
            }
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to save checkpoint [key=" + key + ", val=" + state + ", scope=" +
                scope + ", timeout=" + timeout + ']', e);
        }

        return saved;
    }

    /**
     * @param key Checkpoint key.
     * @return Whether or not checkpoint was removed.
     */
    public boolean removeCheckpoint(String key) {
        if (!enabled())
            return false;

        assert key != null;

        boolean rmv = false;

        for (CheckpointSpi spi : getSpis())
            if (spi.removeCheckpoint(key))
                rmv = true;

        return rmv;
    }

    /**
     * @param ses Task session.
     * @param key Checkpoint key.
     * @return Whether or not checkpoint was removed.
     */
    public boolean removeCheckpoint(GridTaskSessionInternal ses, String key) {
        if (!enabled())
            return false;

        assert ses != null;
        assert key != null;

        Set<String> keys = keyMap.get(ses.getId());

        boolean rmv = false;

        // Note: Check that keys exists because session may be invalidated during removing
        // checkpoint from GridFuture.
        if (keys != null) {
            keys.remove(key);

            rmv = getSpi(ses.getCheckpointSpi()).removeCheckpoint(key);
        }
        else if (log.isDebugEnabled())
            log.debug("Checkpoint will not be removed (key map not found) [key=" + key + ", ses=" + ses + ']');

        return rmv;
    }

    /**
     * @param ses Task session.
     * @param key Checkpoint key.
     * @return Loaded checkpoint.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Nullable public Serializable loadCheckpoint(GridTaskSessionInternal ses, String key) throws IgniteCheckedException {
        if (!enabled())
            return null;

        assert ses != null;
        assert key != null;

        try {
            byte[] data = getSpi(ses.getCheckpointSpi()).loadCheckpoint(key);

            Serializable state = null;

            // Always deserialize with task/session class loader.
            if (data != null)
                state = marsh.unmarshal(data, ses.getClassLoader());

            record(EVT_CHECKPOINT_LOADED, key);

            return state;
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to load checkpoint: " + key, e);
        }
    }

    /**
     * @param ses Task session.
     * @param cleanup Whether cleanup or not.
     */
    public void onSessionEnd(GridTaskSessionInternal ses, boolean cleanup) {
        if (!enabled())
            return;

        closedSess.add(ses.getId());

        // If on task node.
        if (ses.getJobId() == null) {
            Set<String> keys = keyMap.remove(ses.getId());

            if (keys != null) {
                for (String key : keys)
                    getSpi(ses.getCheckpointSpi()).removeCheckpoint(key);
            }
        }
        // If on job node.
        else if (cleanup) {
            // Clean up memory.
            CheckpointSet keys = keyMap.get(ses.getId());

            // Make sure that we don't remove checkpoint set that
            // was created by newly created session.
            if (keys != null && keys.session() == ses.session())
                keyMap.remove(ses.getId(), keys);
        }
    }

    /**
     * @param type Event type.
     * @param key Checkpoint key.
     */
    private void record(int type, String key) {
        if (ctx.event().isRecordable(type)) {
            String msg;

            if (type == EVT_CHECKPOINT_SAVED)
                msg = "Checkpoint saved: " + key;
            else if (type == EVT_CHECKPOINT_LOADED)
                msg = "Checkpoint loaded: " + key;
            else {
                assert type == EVT_CHECKPOINT_REMOVED : "Invalid event type: " + type;

                msg = "Checkpoint removed: " + key;
            }

            ctx.event().record(new CheckpointEvent(ctx.discovery().localNode(), msg, type, key));
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Checkpoint manager memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  keyMap: " + (keyMap != null ? keyMap.size() : 0));
    }

    /**
     * Checkpoint set.
     */
    private static class CheckpointSet extends GridConcurrentHashSet<String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Session. */
        @GridToStringInclude
        private final GridTaskSessionInternal ses;

        /**
         * @param ses Session.
         */
        private CheckpointSet(GridTaskSessionInternal ses) {
            this.ses = ses;
        }

        /**
         * @return Session.
         */
        GridTaskSessionInternal session() {
            return ses;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CheckpointSet.class, this);
        }
    }

    /**
     *
     */
    private class CheckpointRequestListener implements GridMessageListener {
        /**
         * @param nodeId ID of the node that sent this message.
         * @param msg Received message.
         */
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
        @Override public void onMessage(UUID nodeId, Object msg) {
            GridCheckpointRequest req = (GridCheckpointRequest)msg;

            if (log.isDebugEnabled())
                log.debug("Received checkpoint request: " + req);

            if (!enabled())
                return;

            IgniteUuid sesId = req.getSessionId();

            if (closedSess.contains(sesId)) {
                getSpi(req.getCheckpointSpi()).removeCheckpoint(req.getKey());

                return;
            }

            Set<String> keys = keyMap.get(sesId);

            if (keys == null) {
                GridTaskSessionImpl ses = ctx.session().getSession(sesId);

                if (ses == null) {
                    getSpi(req.getCheckpointSpi()).removeCheckpoint(req.getKey());

                    return;
                }

                Set<String> old = keyMap.putIfAbsent(sesId, (CheckpointSet)(keys = new CheckpointSet(ses)));

                if (old != null)
                    keys = old;
            }

            keys.add(req.getKey());

            // Double check.
            if (closedSess.contains(sesId)) {
                keyMap.remove(sesId, keys);

                getSpi(req.getCheckpointSpi()).removeCheckpoint(req.getKey());
            }
        }
    }
}