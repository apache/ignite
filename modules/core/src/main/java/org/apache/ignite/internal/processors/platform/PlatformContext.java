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

package org.apache.ignite.internal.processors.platform;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryProcessor;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryFilter;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterNodeFilter;
import org.apache.ignite.internal.processors.platform.compute.PlatformJob;
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformStreamReceiver;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManager;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Platform context. Acts as an entry point for platform operations.
 */
public interface PlatformContext {
    /**
     * Gets kernal context.
     *
     * @return Kernal context.
     */
    public GridKernalContext kernalContext();

    /**
     * Gets platform memory manager.
     *
     * @return Memory manager.
     */
    public PlatformMemoryManager memory();

    /**
     * Gets platform callback gateway.
     *
     * @return Callback gateway.
     */
    public PlatformCallbackGateway gateway();

    /**
     * Get memory reader.
     *
     * @param mem Memory.
     * @return Reader.
     */
    public BinaryRawReaderEx reader(PlatformMemory mem);

    /**
     * Get memory reader.
     *
     * @param in Input.
     * @return Reader.
     */
    public BinaryRawReaderEx reader(PlatformInputStream in);

    /**
     * Get memory writer.
     *
     * @param mem Memory.
     * @return Writer.
     */
    public BinaryRawWriterEx writer(PlatformMemory mem);

    /**
     * Get memory writer.
     *
     * @param out Output.
     * @return Writer.
     */
    public BinaryRawWriterEx writer(PlatformOutputStream out);

    /**
     * Sends node info to native platform, if necessary.
     *
     * @param node Node.
     */
    public void addNode(ClusterNode node);

    /**
     * Writes a node id to a stream and sends node info to native platform, if necessary.
     *
     * @param writer Writer.
     * @param node Node.
     */
    public void writeNode(BinaryRawWriterEx writer, ClusterNode node);

    /**
     * Writes multiple node ids to a stream and sends node info to native platform, if necessary.
     *
     * @param writer Writer.
     * @param nodes Nodes.
     */
    public void writeNodes(BinaryRawWriterEx writer, Collection<ClusterNode> nodes);

    /**
     * Process metadata from the platform.
     *
     * @param reader Reader.
     */
    public void processMetadata(BinaryRawReaderEx reader);

    /**
     * Write metadata for the given type ID.
     *
     * @param writer Writer.
     * @param typeId Type ID.
     * @param includeSchemas Whether to include binary object schemas into the result.
     */
    public void writeMetadata(BinaryRawWriterEx writer, int typeId, boolean includeSchemas);

    /**
     * Write all available metadata.
     *
     * @param writer Writer.
     */
    public void writeAllMetadata(BinaryRawWriterEx writer);

    /**
     * Write schema for the given type ID and schema ID.
     *
     * @param writer Writer.
     * @param typeId Type ID.
     * @param schemaId Schema ID.
     */
    public void writeSchema(BinaryRawWriterEx writer, int typeId, int schemaId);

    /**
     * Write cluster metrics.
     *
     * @param writer Writer.
     * @param metrics Metrics.
     */
    public void writeClusterMetrics(BinaryRawWriterEx writer, @Nullable ClusterMetrics metrics);

    /**
     *
     * @param ptr Pointer to continuous query deployed on the platform.
     * @param hasFilter Whether filter exists.
     * @param filter Filter.
     * @return Platform continuous query.
     */
    public PlatformContinuousQuery createContinuousQuery(long ptr, boolean hasFilter, @Nullable Object filter);

    /**
     * Create continuous query filter to be deployed on remote node.
     *
     * @param filter Native filter.
     * @return Filter.
     */
    public PlatformContinuousQueryFilter createContinuousQueryFilter(Object filter);

    /**
     * Create remote message filter.
     *
     * @param filter Native filter.
     * @param ptr Pointer of deployed native filter.
     * @return Filter.
     */
    public PlatformMessageFilter createRemoteMessageFilter(Object filter, long ptr);

    /**
     * Check whether the given event type is supported.
     *
     * @param evtTyp Event type.
     * @return {@code True} if supported.
     */
    public boolean isEventTypeSupported(int evtTyp);

    /**
     * Write event.
     *
     * @param writer Writer.
     * @param evt Event.
     */
    public void writeEvent(BinaryRawWriterEx writer, Event evt);

    /**
     * Create local event filter.
     *
     * @param hnd Native handle.
     * @return Filter.
     */
    public PlatformEventFilterListener createLocalEventFilter(long hnd);

    /**
     * Create remote event filter.
     *
     * @param pred Native predicate.
     * @param types Event types.
     * @return Filter.
     */
    public PlatformEventFilterListener createRemoteEventFilter(Object pred, final int... types);

    /**
     * Create native exception.
     *
     * @param cause Native cause.
     * @return Exception.
     */
    public PlatformNativeException createNativeException(Object cause);

    /**
     * Create job.
     *
     * @param task Task.
     * @param ptr Pointer.
     * @param job Native job.
     * @return job.
     */
    public PlatformJob createJob(Object task, long ptr, @Nullable Object job);

    /**
     * Create closure job.
     *
     * @param task Native task.
     * @param ptr Pointer.
     * @param job Native job.
     * @return Closure job.
     */
    public PlatformJob createClosureJob(Object task, long ptr, Object job);

    /**
     * Create cache entry processor.
     *
     * @param proc Native processor.
     * @param ptr Pointer.
     * @return Entry processor.
     */
    public PlatformCacheEntryProcessor createCacheEntryProcessor(Object proc, long ptr);

    /**
     * Create cache entry filter.
     *
     * @param filter Native filter.
     * @param ptr Pointer.
     * @return Entry filter.
     */
    public PlatformCacheEntryFilter createCacheEntryFilter(Object filter, long ptr);

    /**
     * Create stream receiver.
     *
     * @param rcv Native receiver.
     * @param ptr Pointer.
     * @param keepBinary Keep binary flag.
     * @return Stream receiver.
     */
    public PlatformStreamReceiver createStreamReceiver(Object rcv, long ptr, boolean keepBinary);

    /**
     * Create cluster node filter.
     *
     * @param filter Native filter.
     * @return Cluster node filter.
     */
    public PlatformClusterNodeFilter createClusterNodeFilter(Object filter);

    /**
     * Gets the current platform name.
     *
     * @return Current platform name.
     */
    public String platform();

    /**
     * Gets a value indicating whether current platform supports native cache.
     *
     * @return True when native caching is supported; false otherwise.
     */
    boolean isPlatformCacheSupported();

    /**
     * Updates the platform cache cache.
     *
     * @param cacheId Cache id.
     * @param keyBytes Serialized key to update.
     * @param valBytes Serialized value.
     * @param part Key partition.
     * @param ver Key version.
     */
    public void updatePlatformCache(int cacheId, byte[] keyBytes, byte[] valBytes, int part, AffinityTopologyVersion ver);

    /**
     * Enables thread-local optimization for platform cache update.
     */
    void enableThreadLocalForPlatformCacheUpdate();

    /**
     * Disables thread-local optimization for platform cache update.
     */
    void disableThreadLocalForPlatformCacheUpdate();
}
