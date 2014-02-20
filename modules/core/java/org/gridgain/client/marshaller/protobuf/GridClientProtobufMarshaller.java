// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.marshaller.protobuf;

import com.google.protobuf.*;
import org.gridgain.client.marshaller.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.client.message.protobuf.*;
import org.gridgain.grid.kernal.processors.rest.client.message.protobuf.ClientMessagesProtocols.*;
import org.gridgain.grid.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.client.message.protobuf.ClientMessagesProtocols
    .ObjectWrapperType.*;

/**
 * Client messages marshaller based on protocol buffers compiled code.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"unchecked", "UnnecessaryFullyQualifiedName"})
public class GridClientProtobufMarshaller implements GridClientMarshaller {
    /** Unique marshaller ID. */
    public static final Byte PROTOCOL_ID = 2;

    /** Empty byte array. */
    private static final byte[] EMPTY = new byte[0];

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object obj) throws IOException {
        if (!(obj instanceof GridClientMessage))
            throw new IOException("Message serialization of given type is not supported: " + obj.getClass().getName());

        ObjectWrapperType type = NONE;

        GeneratedMessage wrapped;

        if ((GridClientMessage)obj instanceof GridClientResponse) {
            GridClientResponse bean = (GridClientResponse)obj;

            ProtoResponse.Builder builder = ProtoResponse.newBuilder();

            if (bean.sessionToken() != null)
                builder.setSessionToken(ByteString.copyFrom(bean.sessionToken()));

            builder.setStatus(bean.successStatus());

            if (bean.errorMessage() != null)
                builder.setErrorMessage(bean.errorMessage());

            if (bean.result() != null)
                builder.setResultBean(wrapObject(bean.result()));

            wrapped = builder.build();

            type = RESPONSE;
        }
        else {
            ProtoRequest.Builder reqBuilder = ProtoRequest.newBuilder();

            if (((GridClientMessage)obj).sessionToken() != null)
                reqBuilder.setSessionToken(ByteString.copyFrom(((GridClientMessage)obj).sessionToken()));

            if ((GridClientMessage)obj instanceof GridClientAuthenticationRequest) {
                GridClientAuthenticationRequest req = (GridClientAuthenticationRequest)obj;

                ProtoAuthenticationRequest.Builder builder = ProtoAuthenticationRequest.newBuilder();

                builder.setCredentials(wrapObject(req.credentials()));

                reqBuilder.setBody(builder.build().toByteString());

                type = AUTH_REQUEST;
            }
            else if ((GridClientMessage)obj instanceof GridClientCacheRequest) {
                GridClientCacheRequest req = (GridClientCacheRequest)obj;

                ProtoCacheRequest.Builder builder = ProtoCacheRequest.newBuilder();

                builder.setOperation(cacheOpToProtobuf(req.operation()));

                if (req.cacheName() != null)
                    builder.setCacheName(req.cacheName());

                if (req.key() != null)
                    builder.setKey(wrapObject(req.key()));

                if (req.value() != null)
                    builder.setValue(wrapObject(req.value()));

                if (req.value2() != null)
                    builder.setValue2(wrapObject(req.value2()));

                if (req.values() != null && !req.values().isEmpty())
                    builder.setValues(wrapMap(req.values()));

                if (req.cacheFlagsOn() != 0)
                    builder.setCacheFlagsOn(req.cacheFlagsOn());

                reqBuilder.setBody(builder.build().toByteString());

                type = CACHE_REQUEST;
            }
            else if ((GridClientMessage)obj instanceof GridClientLogRequest) {
                GridClientLogRequest req = (GridClientLogRequest)obj;

                ProtoLogRequest.Builder builder = ProtoLogRequest.newBuilder();

                if (req.path() != null)
                    builder.setPath(req.path());

                builder.setFrom(req.from());
                builder.setTo(req.to());

                reqBuilder.setBody(builder.build().toByteString());

                type = LOG_REQUEST;
            }
            else if ((GridClientMessage)obj instanceof GridClientTaskRequest) {
                GridClientTaskRequest req = (GridClientTaskRequest)obj;

                ProtoTaskRequest.Builder builder = ProtoTaskRequest.newBuilder();

                builder.setTaskName(req.taskName());

                builder.setArgument(wrapObject(req.argument()));

                reqBuilder.setBody(builder.build().toByteString());

                type = TASK_REQUEST;
            }
            else if ((GridClientMessage)obj instanceof GridClientTopologyRequest) {
                GridClientTopologyRequest req = (GridClientTopologyRequest)obj;

                ProtoTopologyRequest.Builder builder = ProtoTopologyRequest.newBuilder();

                builder.setIncludeAttributes(req.includeAttributes());
                builder.setIncludeMetrics(req.includeMetrics());

                if (req.nodeId() != null)
                    builder.setNodeId(req.nodeId().toString());

                if (req.nodeIp() != null)
                    builder.setNodeIp(req.nodeIp());

                reqBuilder.setBody(builder.build().toByteString());

                type = TOPOLOGY_REQUEST;
            }

            wrapped = reqBuilder.build();
        }

        ObjectWrapper.Builder res = ObjectWrapper.newBuilder();

        res.setType(type);
        res.setBinary(wrapped.toByteString());

        return res.build().toByteArray();
    }

    /**
     * Convert from protobuf cache operation.
     *
     * @param val Protobuf cache operation value.
     * @return GridGain cache operation value.
     */
    private static GridClientCacheRequest.GridCacheOperation cacheOpFromProtobuf(ProtoCacheRequest.GridCacheOperation val) {
        switch (val) {
            case PUT:
                return GridClientCacheRequest.GridCacheOperation.PUT;
            case PUT_ALL:
                return GridClientCacheRequest.GridCacheOperation.PUT_ALL;
            case GET:
                return GridClientCacheRequest.GridCacheOperation.GET;
            case GET_ALL:
                return GridClientCacheRequest.GridCacheOperation.GET_ALL;
            case RMV:
                return GridClientCacheRequest.GridCacheOperation.RMV;
            case RMV_ALL:
                return GridClientCacheRequest.GridCacheOperation.RMV_ALL;
            case REPLACE:
                return GridClientCacheRequest.GridCacheOperation.REPLACE;
            case CAS:
                return GridClientCacheRequest.GridCacheOperation.CAS;
            case METRICS:
                return GridClientCacheRequest.GridCacheOperation.METRICS;
            case APPEND:
                return GridClientCacheRequest.GridCacheOperation.APPEND;
            case PREPEND:
                return GridClientCacheRequest.GridCacheOperation.PREPEND;
            default:
                throw new IllegalArgumentException("Unsupported cache operation: " + val);
        }
    }

    /**
     * Convert to protobuf cache operation.
     *
     * @param val Operation code value.
     * @return Enum value.
     */
    private static ProtoCacheRequest.GridCacheOperation cacheOpToProtobuf(GridClientCacheRequest.GridCacheOperation val) {
        switch (val) {
            case PUT:
                return ProtoCacheRequest.GridCacheOperation.PUT;
            case PUT_ALL:
                return ProtoCacheRequest.GridCacheOperation.PUT_ALL;
            case GET:
                return ProtoCacheRequest.GridCacheOperation.GET;
            case GET_ALL:
                return ProtoCacheRequest.GridCacheOperation.GET_ALL;
            case RMV:
                return ProtoCacheRequest.GridCacheOperation.RMV;
            case RMV_ALL:
                return ProtoCacheRequest.GridCacheOperation.RMV_ALL;
            case REPLACE:
                return ProtoCacheRequest.GridCacheOperation.REPLACE;
            case CAS:
                return ProtoCacheRequest.GridCacheOperation.CAS;
            case METRICS:
                return ProtoCacheRequest.GridCacheOperation.METRICS;
            case APPEND:
                return ProtoCacheRequest.GridCacheOperation.APPEND;
            case PREPEND:
                return ProtoCacheRequest.GridCacheOperation.PREPEND;
            default:
                throw new IllegalArgumentException("Unsupported cache operation: " + val);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        ObjectWrapper msg = ObjectWrapper.parseFrom(in);

        switch (msg.getType()) {
            case CACHE_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoCacheRequest reqBean = ProtoCacheRequest.parseFrom(req.getBody());

                GridClientCacheRequest res = new GridClientCacheRequest(cacheOpFromProtobuf(reqBean.getOperation()));

                fillClientMessage(res, req);

                if (reqBean.hasCacheName())
                    res.cacheName(reqBean.getCacheName());

                if (reqBean.hasKey())
                    res.key(unwrapObject(reqBean.getKey()));

                if (reqBean.hasValue())
                    res.value(unwrapObject(reqBean.getValue()));

                if (reqBean.hasValue2())
                    res.value2(unwrapObject(reqBean.getValue2()));

                if (reqBean.hasCacheFlagsOn())
                    res.cacheFlagsOn(reqBean.getCacheFlagsOn());

                res.values(unwrapMap(reqBean.getValues()));

                return (T)res;
            }

            case TASK_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoTaskRequest reqBean = ProtoTaskRequest.parseFrom(req.getBody());

                GridClientTaskRequest res = new GridClientTaskRequest();

                fillClientMessage(res, req);

                res.taskName(reqBean.getTaskName());

                res.argument(unwrapObject(reqBean.getArgument()));

                return (T)res;
            }

            case LOG_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoLogRequest reqBean = ProtoLogRequest.parseFrom(req.getBody());

                GridClientLogRequest res = new GridClientLogRequest();

                fillClientMessage(res, req);

                if (reqBean.hasPath())
                    res.path(reqBean.getPath());

                res.from(reqBean.getFrom());

                res.to(reqBean.getTo());

                return (T)res;
            }

            case TOPOLOGY_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoTopologyRequest reqBean = ProtoTopologyRequest.parseFrom(req.getBody());

                GridClientTopologyRequest res = new GridClientTopologyRequest();

                fillClientMessage(res, req);

                if (reqBean.hasNodeId())
                    res.nodeId(java.util.UUID.fromString(reqBean.getNodeId()));

                if (reqBean.hasNodeIp())
                    res.nodeIp(reqBean.getNodeIp());

                res.includeAttributes(reqBean.getIncludeAttributes());

                res.includeMetrics(reqBean.getIncludeMetrics());

                return (T)res;
            }

            case AUTH_REQUEST: {
                ProtoRequest req = ProtoRequest.parseFrom(msg.getBinary());

                ProtoAuthenticationRequest reqBean = ProtoAuthenticationRequest.parseFrom(req.getBody());

                GridClientAuthenticationRequest res = new GridClientAuthenticationRequest();

                fillClientMessage(res, req);

                res.credentials(unwrapObject(reqBean.getCredentials()));

                return (T)res;
            }

            case RESPONSE: {
                ProtoResponse resBean = ProtoResponse.parseFrom(msg.getBinary());

                GridClientResponse res = new GridClientResponse();

                if (resBean.hasSessionToken())
                    res.sessionToken(resBean.getSessionToken().toByteArray());

                res.successStatus(resBean.getStatus());

                if (resBean.hasErrorMessage())
                    res.errorMessage(resBean.getErrorMessage());

                if (resBean.hasResultBean())
                    res.result(unwrapObject(resBean.getResultBean()));

                return (T)res;
            }

            default:
                throw new IOException("Failed to unmarshal message (invalid message type was received): " +
                    msg.getType());
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolId() {
        return PROTOCOL_ID;
    }

    /**
     * Fills given client message with values from packet.
     *
     * @param res Resulting message to fill.
     * @param req Packet to get values from.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void fillClientMessage(GridClientMessage res, ProtoRequest req) {
        if (req.hasSessionToken())
            res.sessionToken(req.getSessionToken().toByteArray());
    }

    /**
     * Converts node bean to protocol message.
     *
     * @param node Node bean to convert.
     * @return Converted message.
     * @throws IOException If node attribute cannot be converted.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private ProtoNodeBean wrapNode(GridClientNodeBean node) throws IOException {
        ProtoNodeBean.Builder builder = ProtoNodeBean.newBuilder();

        builder.setNodeId(wrapUUID(node.getNodeId()));
        builder.setConsistentId(wrapObject(node.getConsistentId()));

        builder.setTcpPort(node.getTcpPort());
        builder.setJettyPort(node.getJettyPort());

        builder.addAllJettyAddress(node.getJettyAddresses());
        builder.addAllJettyHostName(node.getJettyHostNames());
        builder.addAllTcpAddress(node.getTcpAddresses());
        builder.addAllTcpHostName(node.getTcpHostNames());

        if (node.getDefaultCacheMode() != null || node.getCaches() != null) {
            java.util.Map<String, String> caches = new HashMap<>();

            if (node.getDefaultCacheMode() != null)
                caches.put(null, node.getDefaultCacheMode());

            if (node.getCaches() != null)
                caches.putAll(node.getCaches());

            builder.setCaches(wrapMap(caches));
        }

        builder.setReplicaCount(node.getReplicaCount());

        if (node.getAttributes() != null && !node.getAttributes().isEmpty())
            builder.setAttributes(wrapMap(node.getAttributes()));

        if (node.getMetrics() != null) {
            ProtoNodeMetricsBean.Builder metricsBean = ProtoNodeMetricsBean.newBuilder();

            GridClientNodeMetricsBean metrics = node.getMetrics();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setFileSystemFreeSpace(metrics.getFileSystemFreeSpace());
            metricsBean.setFileSystemTotalSpace(metrics.getFileSystemTotalSpace());
            metricsBean.setFileSystemUsableSpace(metrics.getFileSystemUsableSpace());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setUpTime(metrics.getUpTime());

            builder.setMetrics(metricsBean.build());
        }

        return builder.build();
    }

    /**
     * Converts protocol message to a node bean.
     *
     * @param bean Protocol message.
     * @return Converted node bean.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private GridClientNodeBean unwrapNode(ProtoNodeBean bean) throws IOException {
        GridClientNodeBean res = new GridClientNodeBean();

        res.setNodeId(unwrapUUID(bean.getNodeId()));
        res.setConsistentId(unwrapObject(bean.getConsistentId()));

        res.setTcpPort(bean.getTcpPort());
        res.setJettyPort(bean.getJettyPort());

        res.setJettyAddresses(bean.getJettyAddressList());
        res.setJettyHostNames(bean.getJettyHostNameList());
        res.setTcpAddresses(bean.getTcpAddressList());
        res.setTcpHostNames(bean.getTcpHostNameList());

        if (bean.hasCaches()) {
            java.util.Map<String, String> caches = (java.util.Map<String, String>)unwrapMap(bean.getCaches());

            if (caches.containsKey(null))
                res.setDefaultCacheMode(caches.remove(null));

            if (!caches.isEmpty())
                res.setCaches(caches);
        }

        res.setReplicaCount(bean.getReplicaCount());

        res.setAttributes((java.util.Map<String, Object>)unwrapMap(bean.getAttributes()));

        if (bean.hasMetrics()) {
            ProtoNodeMetricsBean metrics = bean.getMetrics();

            GridClientNodeMetricsBean metricsBean = new GridClientNodeMetricsBean();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentGcCpuLoad(metrics.getCurrentGcCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setFileSystemFreeSpace(metrics.getFileSystemFreeSpace());
            metricsBean.setFileSystemTotalSpace(metrics.getFileSystemTotalSpace());
            metricsBean.setFileSystemUsableSpace(metrics.getFileSystemUsableSpace());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setTotalExecutedTasks(metrics.getTotalExecutedTasks());
            metricsBean.setSentMessagesCount(metrics.getSentMessagesCount());
            metricsBean.setSentBytesCount(metrics.getSentBytesCount());
            metricsBean.setReceivedMessagesCount(metrics.getReceivedMessagesCount());
            metricsBean.setReceivedBytesCount(metrics.getReceivedBytesCount());

            metricsBean.setUpTime(metrics.getUpTime());

            res.setMetrics(metricsBean);
        }

        return res;
    }

    /**
     * Wraps task result bean into a protocol message.
     *
     * @param bean Task result that need to be wrapped.
     * @return Wrapped message.
     * @throws IOException If result object cannot be serialized.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private ProtoTaskBean wrapTaskResult(GridClientTaskResultBean bean) throws IOException {
        ProtoTaskBean.Builder builder = ProtoTaskBean.newBuilder();

        builder.setTaskId(bean.getId());
        builder.setFinished(bean.isFinished());

        if (bean.getError() != null)
            builder.setError(bean.getError());

        if (bean.getResult() != null)
            builder.setResultBean(wrapObject(bean.getResult()));

        return builder.build();
    }

    /**
     * Unwraps protocol message to a task result bean.
     *
     * @param bean Protocol message to unwrap.
     * @return Unwrapped message.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private GridClientTaskResultBean unwrapTaskResult(ProtoTaskBean bean) throws IOException {
        GridClientTaskResultBean res = new GridClientTaskResultBean();

        res.setId(bean.getTaskId());
        res.setFinished(bean.getFinished());

        if (bean.hasError())
            res.setError(bean.getError());

        if (bean.hasResultBean())
            res.setResult(unwrapObject(bean.getResultBean()));

        return res;
    }

    /**
     * Converts java object to a protocol-understandable format.
     *
     * @param obj Object to convert.
     * @return Wrapped protocol message object.
     * @throws IOException If object of given type cannot be converted.
     */
    private ObjectWrapper wrapObject(Object obj) throws IOException {
        ObjectWrapper.Builder builder = ObjectWrapper.newBuilder();

        ObjectWrapperType type = NONE;
        ByteString data;

        if (obj == null) {
            builder.setType(type);

            builder.setBinary(ByteString.copyFrom(EMPTY));

            return builder.build();
        }
        else if (obj instanceof Boolean) {
            data = ByteString.copyFrom((Boolean)obj ? new byte[] {0x01} : new byte[] {0x00});

            type = BOOL;
        }
        else if (obj instanceof Byte) {
            data = ByteString.copyFrom(new byte[] {(Byte)obj});

            type = BYTE;
        }
        else if (obj instanceof Short) {
            data = ByteString.copyFrom(GridClientByteUtils.shortToBytes((Short)obj));

            type = SHORT;
        }
        else if (obj instanceof Integer) {
            data = ByteString.copyFrom(GridClientByteUtils.intToBytes((Integer)obj));

            type = INT32;
        }
        else if (obj instanceof Long) {
            data = ByteString.copyFrom(GridClientByteUtils.longToBytes((Long)obj));

            type = INT64;
        }
        else if (obj instanceof Float) {
            data = ByteString.copyFrom(GridClientByteUtils.intToBytes(Float.floatToIntBits((Float)obj)));

            type = FLOAT;
        }
        else if (obj instanceof Double) {
            data = ByteString.copyFrom(GridClientByteUtils.longToBytes(Double.doubleToLongBits((Double)obj)));

            type = DOUBLE;
        }
        else if (obj instanceof String) {
            data = ByteString.copyFrom((String)obj, "UTF-8");

            type = STRING;
        }
        else if (obj instanceof byte[]) {
            data = ByteString.copyFrom((byte[])obj);

            type = BYTES;
        }
        else if (obj instanceof java.util.Collection) {
            data = wrapCollection((java.util.Collection)obj).toByteString();

            type = COLLECTION;
        }
        else if (obj instanceof java.util.Map) {
            data = wrapMap((java.util.Map)obj).toByteString();

            type = MAP;
        }
        else if (obj instanceof java.util.UUID) {
            data = wrapUUID((java.util.UUID)obj);

            type = UUID;
        }
        else if (obj instanceof GridClientNodeBean) {
            data = wrapNode((GridClientNodeBean)obj).toByteString();

            type = NODE_BEAN;
        }
        else if (obj instanceof GridClientTaskResultBean) {
            data = wrapTaskResult((GridClientTaskResultBean)obj).toByteString();

            type = TASK_BEAN;
        }
        // To-string conversion for special cases
        else if (obj instanceof Enum || obj instanceof InetAddress) {
            data = ByteString.copyFrom(obj.toString(), "UTF-8");
        }
        else if (obj.getClass().isArray()) {
            throw new IllegalArgumentException("Failed to serialize array (use collections instead): "
                + obj.getClass().getName());
        }
        else if (obj instanceof Serializable) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(out);

            objOut.writeObject(obj);
            objOut.close();

            data = ByteString.copyFrom(out.toByteArray());

            type = SERIALIZABLE;
        }
        else
            throw new IllegalArgumentException("Failed to serialize object " +
                "(object serialization of given type is not supported): " + obj.getClass().getName());

        builder.setType(type);
        builder.setBinary(data);

        return builder.build();
    }

    /**
     * Unwraps protocol value to a Java type.
     *
     * @param wrapper Wrapped value.
     * @return Corresponding Java object.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "ConstantConditions"})
    private Object unwrapObject(ObjectWrapper wrapper) throws IOException {
        ObjectWrapperType type = wrapper.getType();

        ByteString data = wrapper.getBinary();

        switch (type) {
            case NONE: {
                return null;
            }

            case BOOL: {
                return data.byteAt(0) != 0x00;
            }

            case BYTE: {
                return data.byteAt(0);
            }

            case SHORT: {
                return GridClientByteUtils.bytesToShort(data.toByteArray(), 0);
            }

            case INT32: {
                return GridClientByteUtils.bytesToInt(data.toByteArray(), 0);
            }

            case INT64: {
                return GridClientByteUtils.bytesToLong(data.toByteArray(), 0);
            }

            case FLOAT: {
                return Float.intBitsToFloat(GridClientByteUtils.bytesToInt(data.toByteArray(), 0));
            }

            case DOUBLE: {
                return Double.longBitsToDouble(GridClientByteUtils.bytesToLong(data.toByteArray(), 0));
            }

            case BYTES: {
                return data.toByteArray();
            }

            case STRING: {
                return data.toStringUtf8();
            }

            case COLLECTION: {
                return unwrapCollection(ClientMessagesProtocols.Collection.parseFrom(data));
            }

            case MAP: {
                return unwrapMap(ClientMessagesProtocols.Map.parseFrom(data));
            }

            case UUID: {
                return unwrapUUID(data);
            }

            case NODE_BEAN: {
                return unwrapNode(ProtoNodeBean.parseFrom(data));
            }

            case TASK_BEAN: {
                return unwrapTaskResult(ProtoTaskBean.parseFrom(data));
            }

            case SERIALIZABLE: {
                ByteArrayInputStream in = new ByteArrayInputStream(data.toByteArray());
                ObjectInputStream objIn = new ObjectInputStream(in);

                try {
                    return objIn.readObject();
                }
                catch (ClassNotFoundException e) {
                    throw new IOException(e.getMessage(), e);
                }
            }

            default:
                throw new IOException("Failed to unmarshal object (unsupported type): " + type);
        }
    }

    /**
     * Converts map to a sequence of key-value pairs.
     *
     * @param map Map to convert.
     * @return Sequence of key-value pairs.
     * @throws IOException If some key or value in map cannot be converted.
     */
    private ClientMessagesProtocols.Map wrapMap(java.util.Map<?, ?> map) throws IOException {
        ClientMessagesProtocols.Map.Builder builder = ClientMessagesProtocols.Map.newBuilder();

        if (map != null && !map.isEmpty()) {
            java.util.Collection<KeyValue> res = new ArrayList<>(map.size());

            for (java.util.Map.Entry o : map.entrySet()) {
                KeyValue.Builder entryBuilder = KeyValue.newBuilder();

                entryBuilder.setKey(wrapObject(o.getKey())).setValue(wrapObject(o.getValue()));

                res.add(entryBuilder.build());
            }

            builder.addAllEntry(res);
        }

        return builder.build();
    }

    /**
     * Converts collection to a sequence of wrapped objects.
     *
     * @param col Collection to wrap.
     * @return Collection of wrapped objects.
     * @throws IOException If some element of collection cannot be converted.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private ClientMessagesProtocols.Collection wrapCollection(java.util.Collection<?> col) throws IOException {
        ClientMessagesProtocols.Collection.Builder  builder = ClientMessagesProtocols.Collection.newBuilder();

        java.util.Collection<ObjectWrapper> res = new ArrayList<>(col.size());

        for (Object o : col)
            res.add(wrapObject(o));

        builder.addAllItem(res);

        return builder.build();
    }

    /**
     * Converts collection of object wrappers to a sequence of java objects.
     *
     * @param col Collection of object wrappers.
     * @return Collection of java objects.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private java.util.Collection<?> unwrapCollection(ClientMessagesProtocols.Collection col) throws IOException {
        java.util.Collection<Object> res = new ArrayList<>(col.getItemCount());

        for (ObjectWrapper o : col.getItemList())
            res.add(unwrapObject(o));

        return res;
    }

    /**
     * Constructs a Java map from given key-value sequence.
     *
     * @param vals Key-value sequence.
     * @return Constructed map.
     * @throws IOException If message parsing failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private java.util.Map<?, ?> unwrapMap(ClientMessagesProtocols.Map vals) throws IOException {
        java.util.Map<Object, Object> res = new HashMap<>();

        for (KeyValue val : vals.getEntryList())
            res.put(unwrapObject(val.getKey()), unwrapObject(val.getValue()));

        return res;
    }

    /**
     * Converts UUID to a byte array.
     *
     * @param uuid UUID to convert.
     * @return Converted bytes.
     */
    private ByteString wrapUUID(UUID uuid) {
        byte[] buf = new byte[16];

        GridClientByteUtils.uuidToBytes(uuid, buf, 0);

        return ByteString.copyFrom(buf);
    }

    /**
     * Converts byte array to a UUID.
     *
     * @param uuid UUID bytes.
     * @return Corresponding UUID.
     */
    private UUID unwrapUUID(ByteString uuid) {
        return GridClientByteUtils.bytesToUuid(uuid.toByteArray(), 0);
    }
}
