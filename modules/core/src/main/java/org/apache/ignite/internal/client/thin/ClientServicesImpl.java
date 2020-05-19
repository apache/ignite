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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.platform.PlatformServiceMethod;

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
        A.notNullOrEmpty(name, "name");
        A.notNull(svcItf, "svcItf");

        Collection<UUID> nodeIds = grp.nodeIds();

        if (nodeIds != null && nodeIds.isEmpty())
            throw new ClientException("Cluster group is empty.");

        return (T)Proxy.newProxyInstance(svcItf.getClassLoader(), new Class[] {svcItf},
            new ServiceInvocationHandler<>(name, timeout, nodeIds));
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
        /** Service name. */
        private final String name;

        /** Timeout. */
        private final long timeout;

        /** Node IDs. */
        private final Collection<UUID> nodeIds;

        /**
         * @param name Service name.
         * @param timeout Timeout.
         */
        private ServiceInvocationHandler(String name, long timeout, Collection<UUID> nodeIds) {
            this.name = name;
            this.timeout = timeout;
            this.nodeIds = nodeIds;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return ch.service(ClientOperation.SERVICE_INVOKE,
                    req -> writeServiceInvokeRequest(req, method, args),
                    res -> utils.readObject(res.in(), false)
                );
            }
            catch (ClientError e) {
                throw new ClientException(e);
            }
        }

        /**
         * @param ch Payload output channel.
         */
        private void writeServiceInvokeRequest(PayloadOutputChannel ch, Method method, Object[] args) {
            ch.clientChannel().protocolCtx().checkFeatureSupported(ProtocolBitmaskFeature.SERVICE_INVOKE);

            try (BinaryRawWriterEx writer = utils.createBinaryWriter(ch.out())) {
                writer.writeString(name);
                writer.writeByte((byte)0); // Flags.
                writer.writeLong(timeout);

                if (nodeIds == null)
                    writer.writeInt(0);
                else {
                    writer.writeInt(nodeIds.size());

                    for (UUID nodeId : nodeIds)
                        writer.writeUuid(nodeId);
                }

                PlatformServiceMethod ann = method.getDeclaredAnnotation(PlatformServiceMethod.class);

                writer.writeString(ann != null ? ann.value() : method.getName());

                Class<?>[] paramTypes = method.getParameterTypes();
                int[] paramTypeIds = new int[paramTypes.length];

                for (int i = 0; i < paramTypes.length; i++)
                    paramTypeIds[i] = marsh.context().typeId(paramTypes[i].getName());

                writer.writeIntArray(paramTypeIds);

                if (F.isEmpty(args))
                    writer.writeInt(0);
                else {
                    writer.writeInt(args.length);

                    for (Object arg : args)
                        writer.writeObject(arg);
                }
            }
        }
    }
}
