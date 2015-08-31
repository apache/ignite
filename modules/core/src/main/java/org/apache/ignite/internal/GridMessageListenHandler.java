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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous handler for message subscription.
 */
public class GridMessageListenHandler implements GridContinuousHandler {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Object topic;

    /** */
    private IgniteBiPredicate<UUID, Object> pred;

    /** */
    private byte[] topicBytes;

    /** */
    private byte[] predBytes;

    /** */
    private String clsName;

    /** */
    private GridDeploymentInfoBean depInfo;

    /** */
    private boolean depEnabled;

    /**
     * Required by {@link Externalizable}.
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
    @Override public boolean isForEvents() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isForMessaging() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isForQuery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(UUID nodeId, UUID routineId, final GridKernalContext ctx) throws IgniteCheckedException {
        ctx.io().addUserMessageListener(topic, pred);

        return RegisterStatus.REGISTERED;
    }

    /** {@inheritDoc} */
    @Override public void onListenerRegistered(UUID routineId, GridKernalContext ctx) {
        // No-op.
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
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (topic != null)
            topicBytes = ctx.config().getMarshaller().marshal(topic);

        predBytes = ctx.config().getMarshaller().marshal(pred);

        // Deploy only listener, as it is very likely to be of some user class.
        GridPeerDeployAware pda = U.peerDeployAware(pred);

        clsName = pda.deployClass().getName();

        GridDeployment dep = ctx.deploy().deploy(pda.deployClass(), pda.classLoader());

        if (dep == null)
            throw new IgniteDeploymentCheckedException("Failed to deploy message listener.");

        depInfo = new GridDeploymentInfoBean(dep);

        depEnabled = true;
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        assert nodeId != null;
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
            depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

        if (dep == null)
            throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

        ClassLoader ldr = dep.classLoader();

        if (topicBytes != null)
            topic = ctx.config().getMarshaller().unmarshal(topicBytes, ldr);

        pred = ctx.config().getMarshaller().unmarshal(predBytes, ldr);
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(depEnabled);

        if (depEnabled) {
            U.writeByteArray(out, topicBytes);
            U.writeByteArray(out, predBytes);
            U.writeString(out, clsName);
            out.writeObject(depInfo);
        }
        else {
            out.writeObject(topic);
            out.writeObject(pred);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        depEnabled = in.readBoolean();

        if (depEnabled) {
            topicBytes = U.readByteArray(in);
            predBytes = U.readByteArray(in);
            clsName = U.readString(in);
            depInfo = (GridDeploymentInfoBean)in.readObject();
        }
        else {
            topic = in.readObject();
            pred = (IgniteBiPredicate<UUID, Object>)in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMessageListenHandler.class, this);
    }
}