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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatchAdapter;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous handler for message subscription.
 */
public final class GridMessageListenHandler implements GridContinuousHandler, MarshallableMessage {
    /** */
    private volatile @Nullable Object topic;

    /** Marshalled {@link #topic}. */
    @Order(0)
    @Nullable volatile byte[] topicBytes;

    /** */
    private volatile IgniteBiPredicate<UUID, Object> pred;

    /** Marshalled {@link #pred}. */
    @Order(1)
    volatile byte[] predBytes;

    /** Class name of {@link #pred}. Is {@code null} if the P2P deployment is disabled. */
    @Order(2)
    @Nullable volatile String clsName;

    /** P2P deploy info of {@link #pred}. Is {@code null} if the P2P deployment is disabled. */
    @Order(3)
    @Nullable volatile GridDeploymentInfoBean predDepInfo;

    /** P2P unmarshalling future. */
    private volatile IgniteInternalFuture<Void> p2pUnmarshalFut = new GridFinishedFuture<>();

    /**
     * Empty constructor for serialization purposes
     */
    public GridMessageListenHandler() {
        // No-op.
    }

    /**
     * @param topic Topic.
     * @param pred Predicate.
     */
    public GridMessageListenHandler(@Nullable Object topic, IgniteBiPredicate<UUID, Object> pred) {
        assert pred != null;

        this.topic = topic;
        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override public boolean isEvents() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isMessaging() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void updateCounters(AffinityTopologyVersion topVer, Map<UUID, Map<Integer, Long>> cntrsPerNode,
        Map<Integer, Long> cntrs) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> updateCounters() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(UUID nodeId, UUID routineId, final GridKernalContext ctx) {
        p2pUnmarshalFut.listen(() -> {
            if (p2pUnmarshalFut.error() == null)
                ctx.io().addUserMessageListener(topic, pred, nodeId);
        });

        return RegisterStatus.REGISTERED;
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID routineId, GridKernalContext ctx) {
        ctx.io().removeUserMessageListener(topic, pred);
    }

    /** {@inheritDoc} */
    @Override public void notifyCallback(UUID nodeId, UUID routineId, Collection<?> objs, GridKernalContext ctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void prepareToMarshal(GridKernalContext ctx, boolean p2p) throws IgniteCheckedException {
        assert ctx != null;

        // TODO : Remove this check after https://issues.apache.org/jira/browse/IGNITE-28945
        if (predBytes != null)
            return;

        if (p2p) {
            assert ctx.config().isPeerClassLoadingEnabled();

            // Deploy only listener, as it is very likely to be of some user class.
            GridPeerDeployAware pda = U.peerDeployAware(pred);

            clsName = pda.deployClass().getName();

            GridDeployment dep = ctx.deploy().deploy(pda.deployClass(), pda.classLoader());

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy message listener.");

            predDepInfo = new GridDeploymentInfoBean(dep);
        }

        if (topic != null)
            topicBytes = U.marshal(ctx.marshaller(), topic);

        predBytes = U.marshal(ctx.marshaller(), pred);
    }

    /** Presents due to {@link MarshallableMessage}'s {@link #unmarshal(Marshaller, ClassLoader)} */
    @Override public void marshal(Marshaller marsh) throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(UUID nodeId, GridKernalContext ctx, boolean p2p) throws IgniteCheckedException {
        assert ctx != null;

        // TODO : Remove this check after https://issues.apache.org/jira/browse/IGNITE-28945
        if (pred != null)
            return;

        if (p2p) {
            assert nodeId != null;
            assert ctx.config().isPeerClassLoadingEnabled();

            try {
                GridDeployment dep = ctx.deploy().getGlobalDeployment(predDepInfo.deployMode(), clsName, clsName,
                    predDepInfo.userVersion(), nodeId, predDepInfo.classLoaderId(), predDepInfo.participants(), null);

                if (dep == null)
                    throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

                ClassLoader ldr = dep.classLoader();

                if (topicBytes != null)
                    topic = U.unmarshal(ctx, topicBytes, U.resolveClassLoader(ldr, ctx.config()));

                pred = U.unmarshal(ctx, predBytes, U.resolveClassLoader(ldr, ctx.config()));
            }
            catch (IgniteCheckedException | IgniteException e) {
                ((GridFutureAdapter)p2pUnmarshalFut).onDone(e);

                throw e;
            }
            catch (ExceptionInInitializerError e) {
                ((GridFutureAdapter)p2pUnmarshalFut).onDone(e);

                throw new IgniteCheckedException("Failed to unmarshal deployable object.", e);
            }

            ((GridFutureAdapter)p2pUnmarshalFut).onDone();
        }
        else {
            if (topicBytes != null)
                topic = U.unmarshal(ctx, topicBytes, U.gridClassLoader());

            pred = U.unmarshal(ctx, predBytes, U.gridClassLoader());
        }
    }

    /** Presents to reset {@link #p2pUnmarshalFut} is case of the P2P-deployment. */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        assert (clsName == null) == (predDepInfo == null);

        /** Are unmarshaled in {@link #finishUnmarshal(UUID, GridKernalContext, boolean)}. */
        if (predDepInfo != null)
            p2pUnmarshalFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public GridContinuousBatch createBatch() {
        return new GridContinuousBatchAdapter();
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onBatchAcknowledged(UUID routineId, GridContinuousBatch batch, GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object orderedTopic() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousHandler clone() {
        try {
            return (GridContinuousHandler)super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMessageListenHandler.class, this);
    }
}
