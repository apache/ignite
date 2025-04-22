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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
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
    /** Max service topology update period in millis. */
    static final int SRV_TOP_UPDATE_PERIOD = 60_000;

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

    /** Services topology. {@code Null} if partition awareness is not enabled. */
    private final Map<String, ServiceTopology> servicesTopologies;

    /** Constructor. */
    ClientServicesImpl(ReliableChannel ch, ClientBinaryMarshaller marsh, ClientClusterGroupImpl grp, IgniteLogger log) {
        this.ch = ch;
        this.marsh = marsh;
        this.grp = grp;

        utils = new ClientUtils(marsh);

        this.log = log;

        servicesTopologies = ch.partitionAwarenessEnabled ? new ConcurrentHashMap<>() : Collections.emptyMap();
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
                try (BinaryRawReaderEx reader = utils.createBinaryReader(res.in())) {
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
                try (BinaryRawReaderEx reader = utils.createBinaryReader(res.in())) {
                    return readServiceDescriptor(reader);
                }
                catch (IOException e) {
                    throw new ClientException(e);
                }
            }
        );
    }

    /** */
    private ClientServiceDescriptorImpl readServiceDescriptor(BinaryRawReaderEx reader) {
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
     * Keeps topology of certain service and its update progress meta.
     */
    private final class ServiceTopology {
        /** The service name. */
        private final String srvcName;

        /** If {@code true}, topology update of current service is in progress. */
        private final AtomicBoolean updateInProgress = new AtomicBoolean();

        /** Time of the last received topology. */
        private volatile long lastUpdateRequestTime;

        /** UUID of the nodes with at least one service instance. */
        private volatile List<UUID> nodes = Collections.emptyList();

        /** Last cluster topology version when current service topology was actual. */
        private volatile AffinityTopologyVersion lastAffTop;

        /** */
        private ServiceTopology(String name) {
            srvcName = name;
        }

        /**
         * @return {@code True} if update of the service topology is required. {@code False} otherwise.
         */
        private boolean isUpdateRequired(AffinityTopologyVersion curAffTop) {
            return lastAffTop == null || curAffTop.topologyVersion() > lastAffTop.topologyVersion()
                || U.nanosToMillis(System.nanoTime() - lastUpdateRequestTime) >= SRV_TOP_UPDATE_PERIOD;
        }

        /**
         * Asynchronously updates the service topology.
         */
        private void updateTopologyAsync() {
            AffinityTopologyVersion curAffTop = ch.affinityContext().lastTopology().version();

            if (!updateInProgress.compareAndSet(false, true))
                return;

            ch.serviceAsync(
                ClientOperation.SERVICE_GET_TOPOLOGY,
                req -> utils.writeObject(req.out(), srvcName),
                resp -> {
                    int cnt = resp.in().readInt();

                    List<UUID> res = new ArrayList<>(cnt);

                    for (int i = 0; i < cnt; ++i)
                        res.add(new UUID(resp.in().readLong(), resp.in().readLong()));

                    return res;
                }).whenComplete((nodes, err) -> {
                    if (err == null) {
                        this.nodes = filterTopology(nodes);
                        lastAffTop = curAffTop;
                        lastUpdateRequestTime = System.nanoTime();

                        if (log.isDebugEnabled()) {
                            log.debug("Topology of service '" + srvcName + "' has been updated. " +
                                "The service instance nodes: " + nodes + ". " +
                                "Effective topology with the cluster group is: " + this.nodes + '.');
                        }
                    }
                    else
                        log.error("Failed to update topology of the service '" + srvcName + "'.", err);

                    updateInProgress.set(false);
                });
        }

        /** */
        private List<UUID> filterTopology(List<UUID> nodes) {
            return Collections.unmodifiableList(grp == null ? nodes : nodes.stream().filter(n -> grp.node(n) != null)
                .collect(Collectors.toList()));
        }

        /**
         * Provides last known service topology and asynchronously updates it if required.
         *
         * @return Last known service topology.
         */
        public List<UUID> getAndUpdate() {
            if (isUpdateRequired(ch.affinityContext().lastTopology().version()))
                updateTopologyAsync();

            return nodes;
        }
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
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                Collection<UUID> nodeIds = grp.nodeIds();

                if (nodeIds != null && nodeIds.isEmpty())
                    throw new ClientException("Cluster group is empty.");

                return ch.service(ClientOperation.SERVICE_INVOKE,
                    req -> writeServiceInvokeRequest(req, nodeIds, method, args),
                    res -> utils.readObject(res.in(), false, method.getReturnType()),
                    serviceTopology()
                );
            }
            catch (ClientError e) {
                throw new ClientException(e);
            }
        }

        /**
         * @return Actual known service topology or empty list if: service topology is not enabled, not supported or
         * not received yet.
         */
        private List<UUID> serviceTopology() {
            if (!ch.partitionAwarenessEnabled
                || !ch.applyOnDefaultChannel(c -> c.protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.SERVICE_TOPOLOGY), null))
                return Collections.emptyList();

            return servicesTopologies.computeIfAbsent(name, ServiceTopology::new).getAndUpdate();
        }

        /**
         * @param ch Payload output channel.
         * @param nodeIds Node IDs.
         * @param method Method to call.
         * @param args Method args.
         */
        private void writeServiceInvokeRequest(
            PayloadOutputChannel ch,
            Collection<UUID> nodeIds,
            Method method,
            Object[] args
        ) {
            ch.clientChannel().protocolCtx().checkFeatureSupported(callAttrs != null ?
                ProtocolBitmaskFeature.SERVICE_INVOKE_CALLCTX : ProtocolBitmaskFeature.SERVICE_INVOKE);

            try (BinaryRawWriterEx writer = utils.createBinaryWriter(ch.out())) {
                writer.writeString(name);
                writer.writeByte(FLAG_PARAMETER_TYPES_MASK); // Flags.
                writer.writeLong(timeout);

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
