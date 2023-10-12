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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.service.ServiceCallContextImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.platform.PlatformServiceMethod;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceCallContextBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ClientServices}.
 */
class ClientServicesImpl implements ClientServices {
    /** Channel. */
    private final ReliableChannel ch;

    /** Binary marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Utils for serialization/deserialization. */
    private final ClientUtils utils;

    /** Cluster group. */
    private final ClientClusterGroupImpl grp;

    /** Constructor. */
    ClientServicesImpl(ReliableChannel ch, ClientBinaryMarshaller marsh, ClientClusterGroupImpl grp) {
        this.ch = ch;
        this.marsh = marsh;
        this.grp = grp;

        utils = new ClientUtils(marsh);
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

        return new ClientServicesImpl(ch, marsh, grp);
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

        /** Service's known topology with the version. */
        private volatile IgniteBiTuple<List<UUID>, Long> srvTop = new IgniteBiTuple<>(Collections.emptyList(), 0L);

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
                IgniteBiTuple<List<UUID>, Long> top = grp.ch.partitionAwarenessEnabled ? srvTop : null;

                Collection<UUID> nodeIds = grp.nodeIds();

                if (nodeIds != null && nodeIds.isEmpty())
                    throw new ClientException("Cluster group is empty.");

                return ch.service(ClientOperation.SERVICE_INVOKE,
                    req -> writeServiceInvokeRequest(req, nodeIds, method, args, top == null ? null : top.get2()),
                    res -> {
                        Object val;
                        long srvTopVersion;
                        UUID[] uuids = null;

                        try (BinaryReaderExImpl reader = utils.createBinaryReader(res.in())) {
                            val = reader.readObject();

                            srvTopVersion = reader.readLong();

                            int uuidsLen = reader.readInt();

                            if (uuidsLen > 0) {
                                uuids = new UUID[uuidsLen];

                                for (int i = 0; i < uuidsLen; ++i)
                                    uuids[i] = new UUID(reader.readLong(), reader.readLong());
                            }
                        }
                        catch (IOException e) {
                            throw new ClientException(e);
                        }

                        if (srvTopVersion >= 0)
                            updateServiceTopology(srvTopVersion, uuids);

                        return val;
                    },
                    top == null || top.get1().isEmpty() ? null : top.get1()
                );
            }
            catch (ClientError e) {
                throw new ClientException(e);
            }
        }

        /** */
        private void updateServiceTopology(long topVer, UUID[] nodes) {
            assert nodes != null || topVer > 0;

            // 0 means not initialized or not reassigned yet. Should be reset.
            if (topVer == 0)
                srvTop = new IgniteBiTuple<>(Collections.emptyList(), 0L);
            else if (topVer > srvTop.get2()) {
                synchronized (grp) {
                    if (topVer > srvTop.get2())
                        srvTop = new IgniteBiTuple<>(Arrays.asList(nodes), topVer);
                }
            }
        }

        /**
         * @param ch Payload output channel.
         * @param nodeIds Node IDs.
         * @param method Method to call.
         * @param args Method args.
         * @param srvTopVer Service topology version.
         */
        private void writeServiceInvokeRequest(
            PayloadOutputChannel ch,
            Collection<UUID> nodeIds,
            Method method,
            Object[] args,
            @Nullable Long srvTopVer
        ) {
            ch.clientChannel().protocolCtx().checkFeatureSupported(callAttrs != null ?
                ProtocolBitmaskFeature.SERVICE_INVOKE_CALLCTX : ProtocolBitmaskFeature.SERVICE_INVOKE);

            try (BinaryRawWriterEx writer = utils.createBinaryWriter(ch.out())) {
                writer.writeString(name);
                writer.writeByte(FLAG_PARAMETER_TYPES_MASK); // Flags.
                writer.writeLong(timeout);

                if (ch.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.SERVICE_MAPPINGS))
                    writer.writeLong(srvTopVer == null ? -1L : srvTopVer);

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
