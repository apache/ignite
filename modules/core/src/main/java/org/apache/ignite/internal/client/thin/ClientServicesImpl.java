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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.service.ServiceCallContextImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.PlatformServiceMethod;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceCallContextBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ClientServices}.
 */
class ClientServicesImpl implements ClientServices {
    /** Max duration in mills before asking service topology update. */
    private static final int SRV_TOP_UPDATE_PERIOD = 10_000;

    /** Max requests number before asking service topology update. */
    private static final int SRV_TOP_UPDATE_MAX_REQUESTS = 1_000;

    /** Channel. */
    private final ReliableChannel ch;

    /** Binary marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Utils for serialization/deserialization. */
    private final ClientUtils utils;

    /** Cluster group. */
    private final ClientClusterGroupImpl grp;

    /** Logger. */
    private final IgniteLogger log;

    /** Constructor. */
    ClientServicesImpl(ReliableChannel ch, ClientBinaryMarshaller marsh, ClientClusterGroupImpl grp, IgniteLogger log) {
        this.ch = ch;
        this.marsh = marsh;
        this.grp = grp;

        utils = new ClientUtils(marsh);

        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup clusterGroup() {
        return grp;
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(String name, Class<? super T> svcItf) {
        return serviceProxy(name, svcItf, 0);
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(String name, Class<? super T> svcItf, long timeout) {
        return serviceProxy(name, svcItf, null, timeout);
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(String name, Class<? super T> svcItf, ServiceCallContext callCtx) {
        return serviceProxy(name, svcItf, callCtx, 0);
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(String name, Class<? super T> svcItf, ServiceCallContext callCtx, long timeout) {
        A.notNullOrEmpty(name, "name");
        A.notNull(svcItf, "svcItf");
        A.ensure(callCtx == null || callCtx instanceof ServiceCallContextImpl,
            "\"callCtx\" has an invalid type. Custom implementation of " + ServiceCallContext.class.getSimpleName() +
                " is not supported. Please use " + ServiceCallContextBuilder.class.getSimpleName() + " to create it.");

        return (T)Proxy.newProxyInstance(svcItf.getClassLoader(), new Class[] {svcItf},
            new ServiceInvocationHandler<>(name, timeout, grp,
                callCtx == null ? null : ((ServiceCallContextImpl)callCtx).values()));
    }

    /** {@inheritDoc} */
    @Override public Collection<ClientServiceDescriptor> serviceDescriptors() {
        return ch.service(ClientOperation.SERVICE_GET_DESCRIPTORS,
            req -> checkGetServiceDescriptorsSupported(req.clientChannel().protocolCtx()),
            res -> {
                try (BinaryReaderExImpl reader = utils.createBinaryReader(res.in())) {
                    int sz = res.in().readInt();

                    Collection<ClientServiceDescriptor> svcs = new ArrayList<>(sz);

                    for (int i = 0; i < sz; i++)
                        svcs.add(readServiceDescriptor(reader));

                    return svcs;
                }
                catch (IOException e) {
                    throw new ClientException(e);
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public ClientServiceDescriptor serviceDescriptor(String name) {
        A.notNullOrEmpty(name, "name");

        return ch.service(ClientOperation.SERVICE_GET_DESCRIPTOR,
            req -> {
                checkGetServiceDescriptorsSupported(req.clientChannel().protocolCtx());

                try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                    writer.writeString(name);
                }
            },
            res -> {
                try (BinaryReaderExImpl reader = utils.createBinaryReader(res.in())) {
                    return readServiceDescriptor(reader);
                }
                catch (IOException e) {
                    throw new ClientException(e);
                }
            }
        );
    }

    /** */
    private ClientServiceDescriptorImpl readServiceDescriptor(BinaryReaderExImpl reader) {
        return new ClientServiceDescriptorImpl(
            reader.readString(),
            reader.readString(),
            reader.readInt(),
            reader.readInt(),
            reader.readString(),
            reader.readUuid(),
            reader.readByte() == 0 ? PlatformType.JAVA : PlatformType.DOTNET
        );
    }

    /**
     * Gets services facade over the specified cluster group.
     *
     * @param grp Cluster group.
     */
    ClientServices withClusterGroup(ClientClusterGroupImpl grp) {
        A.notNull(grp, "grp");

        return new ClientServicesImpl(ch, marsh, grp, log);
    }

    /**
     * Service invocation handler.
     */
    private class ServiceInvocationHandler<T> implements InvocationHandler {
        /** Flag "Has parameter types" mask. */
        private static final byte FLAG_PARAMETER_TYPES_MASK = 0x02;

        /** Service name. */
        private final String name;

        /** Timeout. */
        private final long timeout;

        /** Cluster group. */
        private final ClientClusterGroupImpl grp;

        /** Service call context attributes. */
        private final Map<String, Object> callAttrs;

        /** Last known topology version. {@code Null} if partition awareness is not enabled. */
        private volatile @Nullable AffinityTopologyVersion lastTopVersion;

        /** Service's instance nodes. {@code Null} if partition awareness is not enabled. */
        private volatile @Nullable List<UUID> instanceNodes;

        /** Time of the last topology update. */
        private volatile long lastTopUpdateTime;

        /** Number of requests without asking for service topology change. */
        private final @Nullable AtomicInteger curTopRequestsCnt;

        /** Topology update-in-progress flag. Holds {@code true}, if topology update is already in progress. */
        private final @Nullable AtomicBoolean svcTopUpdateInProgress;

        /**
         * @param name Service name.
         * @param timeout Timeout.
         * @param grp Cluster group.
         * @param callAttrs Service call context attributes.
         */
        private ServiceInvocationHandler(
            String name,
            long timeout,
            ClientClusterGroupImpl grp,
            @Nullable Map<String, Object> callAttrs
        ) {
            this.name = name;
            this.timeout = timeout;
            this.grp = grp;
            this.callAttrs = callAttrs;
            this.curTopRequestsCnt = grp.ch.partitionAwarenessEnabled ? new AtomicInteger() : null;
            this.svcTopUpdateInProgress = grp.ch.partitionAwarenessEnabled ? new AtomicBoolean() : null;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            AffinityTopologyVersion curTop = ch.affinityContext().lastTopology().version();

            boolean requestTopUpdate = requestSrvTop(curTop);

            try {
                Collection<UUID> nodeIds = grp.nodeIds();

                if (nodeIds != null && nodeIds.isEmpty())
                    throw new ClientException("Cluster group is empty.");

                if (requestTopUpdate && log.isDebugEnabled())
                    log.debug("Requesting service topology update for the service '" + name + "' ...");

                return ch.service(ClientOperation.SERVICE_INVOKE,
                    req -> writeServiceInvokeRequest(req, nodeIds, method, args, requestTopUpdate),
                    res -> {
                        Object val;
                        UUID[] uuids = null;

                        try (BinaryReaderExImpl reader = utils.createBinaryReader(res.in())) {
                            val = reader.readObject();

                            if (requestTopUpdate) {
                                int uuidsLen = reader.readInt();

                                if (uuidsLen > 0) {
                                    uuids = new UUID[uuidsLen];

                                    for (int i = 0; i < uuidsLen; ++i)
                                        uuids[i] = new UUID(reader.readLong(), reader.readLong());
                                }
                            }
                        }
                        catch (IOException e) {
                            throw new ClientException(e);
                        }

                        if (!F.isEmpty(uuids))
                            updateSrvTop(uuids, curTop);

                        return val;
                    },
                    instanceNodes
                );
            }
            catch (ClientError e) {
                throw new ClientException(e);
            }
            finally {
                if (requestTopUpdate)
                    svcTopUpdateInProgress.set(false);
            }
        }

        /**
         * @return {@code True} if service topology update is required.
         */
        private boolean requestSrvTop(AffinityTopologyVersion curTop) {
            if (!ch.partitionAwarenessEnabled || svcTopUpdateInProgress.get())
                return false;

            return (lastTopVersion == null
                || curTop.compareTo(lastTopVersion) > 0
                || System.nanoTime() - lastTopUpdateTime > U.millisToNanos(SRV_TOP_UPDATE_PERIOD)
                || curTopRequestsCnt.getAndIncrement() >= SRV_TOP_UPDATE_MAX_REQUESTS)
                && svcTopUpdateInProgress.compareAndSet(false, true);
        }

        /**
         * Updates service topology, the known nodes with the deployed service.
         */
        private void updateSrvTop(UUID[] serviceNodes, AffinityTopologyVersion srvTopVersion) {
            instanceNodes = Stream.of(serviceNodes).collect(Collectors.toList());

            lastTopVersion = srvTopVersion;

            curTopRequestsCnt.set(0);

            lastTopUpdateTime = System.nanoTime();

            svcTopUpdateInProgress.set(false);

            if (log.isDebugEnabled())
                log.debug("Topology of service '" + name + "' has been updated: " + Arrays.toString(serviceNodes));
        }

        /**
         * @param ch Payload output channel.
         * @param nodeIds Node IDs.
         * @param method Method to call.
         * @param args Method args.
         * @param updateSrvTop Update service topology flag.
         */
        private void writeServiceInvokeRequest(
            PayloadOutputChannel ch,
            Collection<UUID> nodeIds,
            Method method,
            Object[] args,
            boolean updateSrvTop
        ) {
            assert !updateSrvTop || ch.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.SERVICE_MAPPINGS);

            ch.clientChannel().protocolCtx().checkFeatureSupported(callAttrs != null ?
                ProtocolBitmaskFeature.SERVICE_INVOKE_CALLCTX : ProtocolBitmaskFeature.SERVICE_INVOKE);

            try (BinaryRawWriterEx writer = utils.createBinaryWriter(ch.out())) {
                writer.writeString(name);
                writer.writeByte(FLAG_PARAMETER_TYPES_MASK); // Flags.
                writer.writeLong(timeout);

                writer.writeBoolean(updateSrvTop);

                if (nodeIds == null)
                    writer.writeInt(0);
                else {
                    writer.writeInt(nodeIds.size());

                    for (UUID nodeId : nodeIds) {
                        writer.writeLong(nodeId.getMostSignificantBits());
                        writer.writeLong(nodeId.getLeastSignificantBits());
                    }
                }

                PlatformServiceMethod ann = method.getDeclaredAnnotation(PlatformServiceMethod.class);

                writer.writeString(ann != null ? ann.value() : method.getName());

                Class<?>[] paramTypes = method.getParameterTypes();

                if (F.isEmpty(args))
                    writer.writeInt(0);
                else {
                    writer.writeInt(args.length);

                    assert args.length == paramTypes.length : "args=" + args.length + ", types=" + paramTypes.length;

                    for (int i = 0; i < args.length; i++) {
                        writer.writeInt(marsh.context().typeId(paramTypes[i].getName()));
                        writer.writeObject(args[i]);
                    }
                }

                if (callAttrs != null)
                    writer.writeMap(callAttrs);
                else if (ch.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.SERVICE_INVOKE_CALLCTX))
                    writer.writeMap(null);
            }
        }
    }

    /**
     * Check that Get Service Descriptors API is supported by server.
     *
     * @param protocolCtx Protocol context.
     */
    private void checkGetServiceDescriptorsSupported(ProtocolContext protocolCtx)
        throws ClientFeatureNotSupportedByServerException {
        if (!protocolCtx.isFeatureSupported(ProtocolBitmaskFeature.GET_SERVICE_DESCRIPTORS))
            throw new ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature.GET_SERVICE_DESCRIPTORS);
    }
}
