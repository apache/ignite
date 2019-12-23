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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Cache Continuous Query Handler with a security subject id.
 */
public class SecurityAwareContinuousQueryHandler<K, V> extends CacheContinuousQueryHandler<K, V> {
    /** Original handler. */
    private CacheContinuousQueryHandler<K, V> original;

    /**
     * Constructor.
     */
    public SecurityAwareContinuousQueryHandler() {
        super();
    }

    /**
     * @param original Original handler.
     * @param subjectId Security subject id.
     */
    public SecurityAwareContinuousQueryHandler(CacheContinuousQueryHandler<K, V> original, UUID subjectId) {
        this.original = original;

        this.original.subjectId = Objects.requireNonNull(subjectId,
            "The parameter 'subjectId' cannot be null.");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(original);

        U.writeUuid(out, original.subjectId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        original = (CacheContinuousQueryHandler<K, V>)in.readObject();

        original.subjectId = U.readUuid(in);
    }

    /** {@inheritDoc} */
    @Override public void internal(boolean internal) {
        original.internal(internal);
    }

    /** {@inheritDoc} */
    @Override public void notifyExisting(boolean notifyExisting) {
        original.notifyExisting(notifyExisting);
    }

    /** {@inheritDoc} */
    @Override public boolean notifyExisting() {
        return original.notifyExisting();
    }

    /** {@inheritDoc} */
    @Override public boolean oldValueRequired() {
        return original.oldValueRequired();
    }

    /** {@inheritDoc} */
    @Override public CacheEntryUpdatedListener<K, V> localListener() {
        return original.localListener();
    }

    /** {@inheritDoc} */
    @Override public void localOnly(boolean locOnly) {
        original.localOnly(locOnly);
    }

    /** {@inheritDoc} */
    @Override public boolean localOnly() {
        return original.localOnly();
    }

    /** {@inheritDoc} */
    @Override public void taskNameHash(int taskHash) {
        original.taskNameHash(taskHash);
    }

    /** {@inheritDoc} */
    @Override public void skipPrimaryCheck(boolean skipPrimaryCheck) {
        original.skipPrimaryCheck(skipPrimaryCheck);
    }

    /** {@inheritDoc} */
    @Override public boolean isEvents() {
        return original.isEvents();
    }

    /** {@inheritDoc} */
    @Override public boolean isMessaging() {
        return original.isMessaging();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuery() {
        return original.isQuery();
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return original.keepBinary();
    }

    /** {@inheritDoc} */
    @Override public void keepBinary(boolean keepBinary) {
        original.keepBinary(keepBinary);
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return original.cacheName();
    }

    /** {@inheritDoc} */
    @Override public void updateCounters(AffinityTopologyVersion topVer,
        Map<UUID, Map<Integer, T2<Long, Long>>> cntrsPerNode,
        Map<Integer, T2<Long, Long>> cntrs) {
        original.updateCounters(topVer, cntrsPerNode, cntrs);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, T2<Long, Long>> updateCounters() {
        return original.updateCounters();
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(UUID nodeId, UUID routineId,
        GridKernalContext ctx) throws IgniteCheckedException {
        return original.register(nodeId, routineId, ctx);
    }

    /** {@inheritDoc} */
    @Override public void initRemoteFilter(CacheEntryEventFilter filter,
        GridKernalContext ctx) throws IgniteCheckedException {
        original.initRemoteFilter(filter, ctx);
    }

    /** {@inheritDoc} */
    @Override public CacheEntryEventFilter getEventFilter() throws IgniteCheckedException {
        return original.getEventFilter();
    }

    /** {@inheritDoc} */
    @Override public CacheEntryEventFilter getEventFilter0() {
        return original.getEventFilter0();
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, ?> getTransformer() {
        return original.getTransformer();
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer.EventListener<?> localTransformedEventListener() {
        return original.localTransformedEventListener();
    }

    /** {@inheritDoc} */
    @Override public void waitTopologyFuture(GridKernalContext ctx) throws IgniteCheckedException {
        original.waitTopologyFuture(ctx);
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID routineId, GridKernalContext ctx) {
        original.unregister(routineId, ctx);
    }

    /** {@inheritDoc} */
    @Override public void notifyCallback(UUID nodeId, UUID routineId, Collection<?> objs,
        GridKernalContext ctx) {
        original.notifyCallback(nodeId, routineId, objs, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean filter(CacheContinuousQueryEvent evt) {
        return original.filter(evt);
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected() {
        original.onClientDisconnected();
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft() {
        original.onNodeLeft();
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        original.p2pMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        original.p2pUnmarshal(nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isMarshalled() {
        return original.isMarshalled();
    }

    /** {@inheritDoc} */
    @Override public <T> T p2pUnmarshal(
        CacheContinuousQueryDeployableObject depObj, UUID nodeId,
        GridKernalContext ctx) throws IgniteCheckedException {
        return original.p2pUnmarshal(depObj, nodeId, ctx);
    }

    /** {@inheritDoc} */
    @Override public GridContinuousBatch createBatch() {
        return original.createBatch();
    }

    /** {@inheritDoc} */
    @Override public void onBatchAcknowledged(UUID routineId,
        GridContinuousBatch batch,
        GridKernalContext ctx) {
        original.onBatchAcknowledged(routineId, batch, ctx);
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object orderedTopic() {
        return original.orderedTopic();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecurityAwareContinuousQueryHandler.class, this);
    }
}
