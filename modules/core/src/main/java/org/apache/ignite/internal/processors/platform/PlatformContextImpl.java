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

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.CheckpointEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.SwapSpaceEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableMetaDataImpl;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectPortableProcessorImpl;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilterImpl;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryProcessor;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryProcessorImpl;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryFilter;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryImpl;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQueryRemoteFilter;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterNodeFilter;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterNodeFilterImpl;
import org.apache.ignite.internal.processors.platform.compute.PlatformAbstractTask;
import org.apache.ignite.internal.processors.platform.compute.PlatformClosureJob;
import org.apache.ignite.internal.processors.platform.compute.PlatformFullJob;
import org.apache.ignite.internal.processors.platform.compute.PlatformJob;
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformStreamReceiver;
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformStreamReceiverImpl;
import org.apache.ignite.internal.processors.platform.events.PlatformEventFilterListenerImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManager;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.apache.ignite.internal.processors.platform.messaging.PlatformMessageFilterImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformReaderBiClosure;
import org.apache.ignite.internal.processors.platform.utils.PlatformReaderClosure;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.portable.PortableMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of platform context.
 */
public class PlatformContextImpl implements PlatformContext {
    /** Supported event types. */
    private static final Set<Integer> evtTyps;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Marshaller. */
    private final GridPortableMarshaller marsh;

    /** Memory manager. */
    private final PlatformMemoryManagerImpl mem;

    /** Callback gateway. */
    private final PlatformCallbackGateway gate;

    /** Cache object processor. */
    private final CacheObjectPortableProcessorImpl cacheObjProc;

    /** Node ids that has been sent to native platform. */
    private final Set<UUID> sentNodes = Collections.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());

    /**
     * Static initializer.
     */
    static {
        Set<Integer> evtTyps0 = new HashSet<>();

        addEventTypes(evtTyps0, EventType.EVTS_CACHE);
        addEventTypes(evtTyps0, EventType.EVTS_CACHE_QUERY);
        addEventTypes(evtTyps0, EventType.EVTS_CACHE_REBALANCE);
        addEventTypes(evtTyps0, EventType.EVTS_CHECKPOINT);
        addEventTypes(evtTyps0, EventType.EVTS_DISCOVERY_ALL);
        addEventTypes(evtTyps0, EventType.EVTS_JOB_EXECUTION);
        addEventTypes(evtTyps0, EventType.EVTS_SWAPSPACE);
        addEventTypes(evtTyps0, EventType.EVTS_TASK_EXECUTION);

        evtTyps = Collections.unmodifiableSet(evtTyps0);
    }

    /**
     * Adds all elements to a set.
     * @param set Set.
     * @param items Items.
     */
    private static void addEventTypes(Set<Integer> set, int[] items) {
        for (int i : items)
            set.add(i);
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param gate Callback gateway.
     * @param mem Memory manager.
     */
    public PlatformContextImpl(GridKernalContext ctx, PlatformCallbackGateway gate, PlatformMemoryManagerImpl mem) {
        this.ctx = ctx;
        this.gate = gate;
        this.mem = mem;

        cacheObjProc = (CacheObjectPortableProcessorImpl)ctx.cacheObjects();

        marsh = cacheObjProc.marshaller();
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public PlatformMemoryManager memory() {
        return mem;
    }

    /** {@inheritDoc} */
    @Override public PlatformCallbackGateway gateway() {
        return gate;
    }

    /** {@inheritDoc} */
    @Override public PortableRawReaderEx reader(PlatformMemory mem) {
        return reader(mem.input());
    }

    /** {@inheritDoc} */
    @Override public PortableRawReaderEx reader(PlatformInputStream in) {
        return marsh.reader(in);
    }

    /** {@inheritDoc} */
    @Override public PortableRawWriterEx writer(PlatformMemory mem) {
        return writer(mem.output());
    }

    /** {@inheritDoc} */
    @Override public PortableRawWriterEx writer(PlatformOutputStream out) {
        return marsh.writer(out);
    }

    /** {@inheritDoc} */
    @Override public void addNode(ClusterNode node) {
        if (node == null || sentNodes.contains(node.id()))
            return;

        // Send node info to the native platform
        try (PlatformMemory mem0 = mem.allocate()) {
            PlatformOutputStream out = mem0.output();

            PortableRawWriterEx w = writer(out);

            w.writeUuid(node.id());

            Map<String, Object> attrs = new HashMap<>(node.attributes());

            Iterator<Map.Entry<String, Object>> attrIter = attrs.entrySet().iterator();

            while (attrIter.hasNext()) {
                Map.Entry<String, Object> entry = attrIter.next();

                Object val = entry.getValue();

                if (val != null && !val.getClass().getName().startsWith("java.lang"))
                    attrIter.remove();
            }

            w.writeMap(attrs);
            w.writeCollection(node.addresses());
            w.writeCollection(node.hostNames());
            w.writeLong(node.order());
            w.writeBoolean(node.isLocal());
            w.writeBoolean(node.isDaemon());
            writeClusterMetrics(w, node.metrics());

            out.synchronize();

            gateway().nodeInfo(mem0.pointer());
        }

        sentNodes.add(node.id());
    }

    /** {@inheritDoc} */
    @Override public void writeNode(PortableRawWriterEx writer, ClusterNode node) {
        if (node == null) {
            writer.writeUuid(null);

            return;
        }

        addNode(node);

        writer.writeUuid(node.id());
    }

    /** {@inheritDoc} */
    @Override public void writeNodes(PortableRawWriterEx writer, Collection<ClusterNode> nodes) {
        if (nodes == null) {
            writer.writeInt(-1);

            return;
        }

        writer.writeInt(nodes.size());

        for (ClusterNode n : nodes) {
            addNode(n);

            writer.writeUuid(n.id());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeClusterMetrics(PortableRawWriterEx writer, @Nullable ClusterMetrics metrics) {
        if (metrics == null)
            writer.writeBoolean(false);
        else {
            writer.writeBoolean(true);

            writer.writeLong(metrics.getLastUpdateTime());
            writer.writeDate(new Date(metrics.getLastUpdateTime()));
            writer.writeInt(metrics.getMaximumActiveJobs());
            writer.writeInt(metrics.getCurrentActiveJobs());
            writer.writeFloat(metrics.getAverageActiveJobs());
            writer.writeInt(metrics.getMaximumWaitingJobs());

            writer.writeInt(metrics.getCurrentWaitingJobs());
            writer.writeFloat(metrics.getAverageWaitingJobs());
            writer.writeInt(metrics.getMaximumRejectedJobs());
            writer.writeInt(metrics.getCurrentRejectedJobs());
            writer.writeFloat(metrics.getAverageRejectedJobs());

            writer.writeInt(metrics.getTotalRejectedJobs());
            writer.writeInt(metrics.getMaximumCancelledJobs());
            writer.writeInt(metrics.getCurrentCancelledJobs());
            writer.writeFloat(metrics.getAverageCancelledJobs());
            writer.writeInt(metrics.getTotalCancelledJobs());

            writer.writeInt(metrics.getTotalExecutedJobs());
            writer.writeLong(metrics.getMaximumJobWaitTime());
            writer.writeLong(metrics.getCurrentJobWaitTime());
            writer.writeDouble(metrics.getAverageJobWaitTime());
            writer.writeLong(metrics.getMaximumJobExecuteTime());

            writer.writeLong(metrics.getCurrentJobExecuteTime());
            writer.writeDouble(metrics.getAverageJobExecuteTime());
            writer.writeInt(metrics.getTotalExecutedTasks());
            writer.writeLong(metrics.getTotalIdleTime());
            writer.writeLong(metrics.getCurrentIdleTime());

            writer.writeInt(metrics.getTotalCpus());
            writer.writeDouble(metrics.getCurrentCpuLoad());
            writer.writeDouble(metrics.getAverageCpuLoad());
            writer.writeDouble(metrics.getCurrentGcCpuLoad());
            writer.writeLong(metrics.getHeapMemoryInitialized());

            writer.writeLong(metrics.getHeapMemoryUsed());
            writer.writeLong(metrics.getHeapMemoryCommitted());
            writer.writeLong(metrics.getHeapMemoryMaximum());
            writer.writeLong(metrics.getHeapMemoryTotal());
            writer.writeLong(metrics.getNonHeapMemoryInitialized());

            writer.writeLong(metrics.getNonHeapMemoryUsed());
            writer.writeLong(metrics.getNonHeapMemoryCommitted());
            writer.writeLong(metrics.getNonHeapMemoryMaximum());
            writer.writeLong(metrics.getNonHeapMemoryTotal());
            writer.writeLong(metrics.getUpTime());

            writer.writeDate(new Date(metrics.getStartTime()));
            writer.writeDate(new Date(metrics.getNodeStartTime()));
            writer.writeInt(metrics.getCurrentThreadCount());
            writer.writeInt(metrics.getMaximumThreadCount());
            writer.writeLong(metrics.getTotalStartedThreadCount());

            writer.writeInt(metrics.getCurrentDaemonThreadCount());
            writer.writeLong(metrics.getLastDataVersion());
            writer.writeInt(metrics.getSentMessagesCount());
            writer.writeLong(metrics.getSentBytesCount());
            writer.writeInt(metrics.getReceivedMessagesCount());

            writer.writeLong(metrics.getReceivedBytesCount());
            writer.writeInt(metrics.getOutboundMessagesQueueSize());

            writer.writeInt(metrics.getTotalNodes());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void processMetadata(PortableRawReaderEx reader) {
        Collection<T4<Integer, String, String, Map<String, Integer>>> metas = PlatformUtils.readCollection(reader,
            new PlatformReaderClosure<T4<Integer, String, String, Map<String, Integer>>>() {
                @Override public T4<Integer, String, String, Map<String, Integer>> read(PortableRawReaderEx reader) {
                    int typeId = reader.readInt();
                    String typeName = reader.readString();
                    String affKey = reader.readString();

                    Map<String, Integer> fields = PlatformUtils.readMap(reader,
                        new PlatformReaderBiClosure<String, Integer>() {
                            @Override public IgniteBiTuple<String, Integer> read(PortableRawReaderEx reader) {
                                return F.t(reader.readString(), reader.readInt());
                            }
                        });

                    return new T4<>(typeId, typeName, affKey, fields);
                }
            }
        );

        for (T4<Integer, String, String, Map<String, Integer>> meta : metas)
            cacheObjProc.updateMetaData(meta.get1(), meta.get2(), meta.get3(), meta.get4());
    }

    /** {@inheritDoc} */
    @Override public void writeMetadata(PortableRawWriterEx writer, int typeId) {
        writeMetadata0(writer, typeId, cacheObjProc.metadata(typeId));
    }

    /** {@inheritDoc} */
    @Override public void writeAllMetadata(PortableRawWriterEx writer) {
        Collection<PortableMetadata> metas = cacheObjProc.metadata();

        writer.writeInt(metas.size());

        for (org.apache.ignite.portable.PortableMetadata m : metas)
            writeMetadata0(writer, cacheObjProc.typeId(m.typeName()), m);
    }

    /**
     * Write portable metadata.
     *
     * @param writer Writer.
     * @param typeId Type id.
     * @param meta Metadata.
     */
    private void writeMetadata0(PortableRawWriterEx writer, int typeId, PortableMetadata meta) {
        if (meta == null)
            writer.writeBoolean(false);
        else {
            writer.writeBoolean(true);

            Map<String, String> metaFields = ((PortableMetaDataImpl)meta).fields0();

            Map<String, Integer> fields = U.newHashMap(metaFields.size());

            for (Map.Entry<String, String> metaField : metaFields.entrySet())
                fields.put(metaField.getKey(), CacheObjectPortableProcessorImpl.fieldTypeId(metaField.getValue()));

            writer.writeInt(typeId);
            writer.writeString(meta.typeName());
            writer.writeString(meta.affinityKeyFieldName());
            writer.writeMap(fields);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformContinuousQuery createContinuousQuery(long ptr, boolean hasFilter,
        @Nullable Object filter) {
        return new PlatformContinuousQueryImpl(this, ptr, hasFilter, filter);
    }

    /** {@inheritDoc} */
    @Override public PlatformContinuousQueryFilter createContinuousQueryFilter(Object filter) {
        return new PlatformContinuousQueryRemoteFilter(filter);
    }

    /** {@inheritDoc} */
    @Override public PlatformMessageFilter createRemoteMessageFilter(Object filter, long ptr) {
        return new PlatformMessageFilterImpl(filter, ptr, this);
    }

    /** {@inheritDoc} */
    @Override public boolean isEventTypeSupported(int evtTyp) {
        return evtTyps.contains(evtTyp);
    }

    /** {@inheritDoc} */
    @Override public void writeEvent(PortableRawWriterEx writer, Event evt) {
        assert writer != null;

        if (evt == null)
        {
            writer.writeInt(-1);

            return;
        }

        EventAdapter evt0 = (EventAdapter)evt;

        if (evt0 instanceof CacheEvent) {
            writer.writeInt(2);
            writeCommonEventData(writer, evt0);

            CacheEvent event0 = (CacheEvent)evt0;

            writer.writeString(event0.cacheName());
            writer.writeInt(event0.partition());
            writer.writeBoolean(event0.isNear());
            writeNode(writer, event0.eventNode());
            writer.writeObject(event0.key());
            PlatformUtils.writeIgniteUuid(writer, event0.xid());
            writer.writeObject(event0.lockId());
            writer.writeObject(event0.newValue());
            writer.writeObject(event0.oldValue());
            writer.writeBoolean(event0.hasOldValue());
            writer.writeBoolean(event0.hasNewValue());
            writer.writeUuid(event0.subjectId());
            writer.writeString(event0.closureClassName());
            writer.writeString(event0.taskName());
        }
        else if (evt0 instanceof CacheQueryExecutedEvent) {
            writer.writeInt(3);
            writeCommonEventData(writer, evt0);

            CacheQueryExecutedEvent event0 = (CacheQueryExecutedEvent)evt0;

            writer.writeString(event0.queryType());
            writer.writeString(event0.cacheName());
            writer.writeString(event0.className());
            writer.writeString(event0.clause());
            writer.writeUuid(event0.subjectId());
            writer.writeString(event0.taskName());
        }
        else if (evt0 instanceof CacheQueryReadEvent) {
            writer.writeInt(4);
            writeCommonEventData(writer, evt0);

            CacheQueryReadEvent event0 = (CacheQueryReadEvent)evt0;

            writer.writeString(event0.queryType());
            writer.writeString(event0.cacheName());
            writer.writeString(event0.className());
            writer.writeString(event0.clause());
            writer.writeUuid(event0.subjectId());
            writer.writeString(event0.taskName());
            writer.writeObject(event0.key());
            writer.writeObject(event0.value());
            writer.writeObject(event0.oldValue());
            writer.writeObject(event0.row());
        }
        else if (evt0 instanceof CacheRebalancingEvent) {
            writer.writeInt(5);
            writeCommonEventData(writer, evt0);

            CacheRebalancingEvent event0 = (CacheRebalancingEvent)evt0;

            writer.writeString(event0.cacheName());
            writer.writeInt(event0.partition());
            writeNode(writer, event0.discoveryNode());
            writer.writeInt(event0.discoveryEventType());
            writer.writeString(event0.discoveryEventName());
            writer.writeLong(event0.discoveryTimestamp());
        }
        else if (evt0 instanceof CheckpointEvent) {
            writer.writeInt(6);
            writeCommonEventData(writer, evt0);

            CheckpointEvent event0 = (CheckpointEvent)evt0;

            writer.writeString(event0.key());
        }
        else if (evt0 instanceof DiscoveryEvent) {
            writer.writeInt(7);
            writeCommonEventData(writer, evt0);

            DiscoveryEvent event0 = (DiscoveryEvent)evt0;

            writeNode(writer, event0.eventNode());
            writer.writeLong(event0.topologyVersion());

            writeNodes(writer, event0.topologyNodes());
        }
        else if (evt0 instanceof JobEvent) {
            writer.writeInt(8);
            writeCommonEventData(writer, evt0);

            JobEvent event0 = (JobEvent)evt0;

            writer.writeString(event0.taskName());
            writer.writeString(event0.taskClassName());
            PlatformUtils.writeIgniteUuid(writer, event0.taskSessionId());
            PlatformUtils.writeIgniteUuid(writer, event0.jobId());
            writeNode(writer, event0.taskNode());
            writer.writeUuid(event0.taskSubjectId());
        }
        else if (evt0 instanceof SwapSpaceEvent) {
            writer.writeInt(9);
            writeCommonEventData(writer, evt0);

            SwapSpaceEvent event0 = (SwapSpaceEvent)evt0;

            writer.writeString(event0.space());
        }
        else if (evt0 instanceof TaskEvent) {
            writer.writeInt(10);
            writeCommonEventData(writer, evt0);

            TaskEvent event0 = (TaskEvent)evt0;

            writer.writeString(event0.taskName());
            writer.writeString(event0.taskClassName());
            PlatformUtils.writeIgniteUuid(writer, event0.taskSessionId());
            writer.writeBoolean(event0.internal());
            writer.writeUuid(event0.subjectId());
        }
        else
            throw new IgniteException("Unsupported event: " + evt);
    }

    /**
     * Write common event data.
     *
     * @param writer Writer.
     * @param evt Event.
     */
    private void writeCommonEventData(PortableRawWriterEx writer, EventAdapter evt) {
        PlatformUtils.writeIgniteUuid(writer, evt.id());
        writer.writeLong(evt.localOrder());
        writeNode(writer, evt.node());
        writer.writeString(evt.message());
        writer.writeInt(evt.type());
        writer.writeString(evt.name());
        writer.writeDate(new Date(evt.timestamp()));
    }

    /** {@inheritDoc} */
    @Override public PlatformEventFilterListener createLocalEventFilter(long hnd) {
        return new PlatformEventFilterListenerImpl(hnd, this);
    }

    /** {@inheritDoc} */
    @Override public PlatformEventFilterListener createRemoteEventFilter(Object pred, int... types) {
        return new PlatformEventFilterListenerImpl(pred, types);
    }

    /** {@inheritDoc} */
    @Override public PlatformNativeException createNativeException(Object cause) {
        return new PlatformNativeException(cause);
    }

    /** {@inheritDoc} */
    @Override public PlatformJob createJob(Object task, long ptr, @Nullable Object job) {
        return new PlatformFullJob(this, (PlatformAbstractTask)task, ptr, job);
    }

    /** {@inheritDoc} */
    @Override public PlatformJob createClosureJob(Object task, long ptr, Object job) {
        return new PlatformClosureJob((PlatformAbstractTask)task, ptr, job);
    }

    /** {@inheritDoc} */
    @Override public PlatformCacheEntryProcessor createCacheEntryProcessor(Object proc, long ptr) {
        return new PlatformCacheEntryProcessorImpl(proc, ptr);
    }

    /** {@inheritDoc} */
    @Override public PlatformCacheEntryFilter createCacheEntryFilter(Object filter, long ptr) {
        return new PlatformCacheEntryFilterImpl(filter, ptr, this);
    }

    /** {@inheritDoc} */
    @Override public PlatformStreamReceiver createStreamReceiver(Object rcv, long ptr, boolean keepPortable) {
        return new PlatformStreamReceiverImpl(rcv, ptr, keepPortable, this);
    }

    /** {@inheritDoc} */
    @Override public PlatformClusterNodeFilter createClusterNodeFilter(Object filter) {
        return new PlatformClusterNodeFilterImpl(filter, this);
    }
}
