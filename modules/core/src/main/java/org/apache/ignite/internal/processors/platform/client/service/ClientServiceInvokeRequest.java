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

package org.apache.ignite.internal.processors.platform.client.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientObjectResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Request to invoke service method.
 */
public class ClientServiceInvokeRequest extends ClientRequest {
    /** Flag keep binary mask. */
    private static final byte FLAG_KEEP_BINARY_MASK = 0x01;

    /** Flag "has parameter types", indicates that method arguments prefixed by typeId to help to resolve methods. */
    private static final byte FLAG_PARAMETER_TYPES_MASK = 0x02;

    /** Methods cache. */
    private static final Map<MethodDescriptor, Method> methodsCache = new ConcurrentHashMap<>();

    /** Service name. */
    private final String name;

    /** Flags. */
    private final byte flags;

    /** Timeout. */
    private final long timeout;

    /** Nodes. */
    private final Collection<UUID> nodeIds;

    /** Method name. */
    private final String methodName;

    /** Method parameter type IDs. */
    private final int[] paramTypeIds;

    /** Service arguments. */
    private final Object[] args;

    /** Objects reader. */
    private final BinaryRawReaderEx reader;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientServiceInvokeRequest(BinaryReaderExImpl reader) {
        super(reader);

        name = reader.readString();

        flags = reader.readByte();

        timeout = reader.readLong();

        int cnt = reader.readInt();

        nodeIds = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            nodeIds.add(new UUID(reader.readLong(), reader.readLong()));

        methodName = reader.readString();

        int argCnt = reader.readInt();

        paramTypeIds = hasParameterTypes() ? new int[argCnt] : null;

        args = new Object[argCnt];

        // We can't deserialize some types (arrays of user defined types for example) from detached objects.
        // On the other hand, deserialize should be done as part of process() call (not in constructor) for proper
        // error handling.
        // To overcome these issues we store binary reader reference, parse request in constructor (by reading detached
        // objects), restore arguments starting position in input stream and deserialize arguments from input stream
        // in process() method.
        this.reader = reader;

        int argsStartPos = reader.in().position();

        for (int i = 0; i < argCnt; i++) {
            if (paramTypeIds != null)
                paramTypeIds[i] = reader.readInt();

            args[i] = reader.readObjectDetached();
        }

        reader.in().position(argsStartPos);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (F.isEmpty(name))
            throw new IgniteException("Service name can't be empty");

        if (F.isEmpty(methodName))
            throw new IgniteException("Method name can't be empty");

        ServiceDescriptor desc = findServiceDescriptor(ctx, name);

        Class<?> svcCls = desc.serviceClass();

        ClusterGroupAdapter grp = ctx.kernalContext().cluster().get();

        if (ctx.securityContext() != null)
            grp = (ClusterGroupAdapter)grp.forSubjectId(ctx.securityContext().subject().id());

        grp = (ClusterGroupAdapter)(nodeIds.isEmpty() ? grp.forServers() : grp.forNodeIds(nodeIds));

        IgniteServices services = grp.services();

        if (!keepBinary() && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (paramTypeIds != null)
                    reader.readInt(); // Skip parameter typeId, we already read it in constructor.

                args[i] = reader.readObject();
            }
        }

        try {
            Object res;

            if (PlatformService.class.isAssignableFrom(svcCls)) {
                PlatformService proxy = services.serviceProxy(name, PlatformService.class, false, timeout);

                res = proxy.invokeMethod(methodName, keepBinary(), !keepBinary(), args);
            }
            else {
                GridServiceProxy<?> proxy = new GridServiceProxy<>(grp, name, Service.class, false, timeout,
                    ctx.kernalContext());

                Method method = resolveMethod(ctx, svcCls);

                res = proxy.invokeMethod(method, args);
            }

            return new ClientObjectResponse(requestId(), res);
        }
        catch (PlatformNativeException e) {
            ctx.kernalContext().log(getClass()).error("Failed to invoke platform service", e);

            throw new IgniteException("Failed to invoke platform service, see server logs for details");
        }
        catch (Throwable e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Keep binary flag.
     */
    private boolean keepBinary() {
        return (flags & FLAG_KEEP_BINARY_MASK) != 0;
    }

    /**
     * "Has parameter types" flag.
     */
    private boolean hasParameterTypes() {
        return (flags & FLAG_PARAMETER_TYPES_MASK) != 0;
    }

    /**
     * @param ctx Connection context.
     * @param name Service name.
     */
    private static ServiceDescriptor findServiceDescriptor(ClientConnectionContext ctx, String name) {
        for (ServiceDescriptor desc : ctx.kernalContext().service().serviceDescriptors()) {
            if (name.equals(desc.name()))
                return desc;
        }

        throw new IgniteException("Service not found: " + name);
    }

    /**
     * Resolve method by method name and parameter types or parameter values.
     */
    private Method resolveMethod(ClientConnectionContext ctx, Class<?> cls) throws ReflectiveOperationException {
        if (paramTypeIds != null) {
            MethodDescriptor desc = new MethodDescriptor(cls, methodName, paramTypeIds);

            Method method = methodsCache.get(desc);

            if (method != null)
                return method;

            IgniteBinary binary = ctx.kernalContext().grid().binary();

            for (Method method0 : cls.getMethods()) {
                if (methodName.equals(method0.getName())) {
                    MethodDescriptor desc0 = MethodDescriptor.forMethod(binary, method0);

                    methodsCache.putIfAbsent(desc0, method0);

                    if (desc0.equals(desc))
                        return method0;
                }
            }

            throw new NoSuchMethodException("Method not found: " + desc);
        }

        // Try to find method by name and parameter values.
        return PlatformServices.getMethod(cls, methodName, args);
    }

    /**
     *
     */
    private static class MethodDescriptor {
        /** Class. */
        private final Class<?> cls;

        /** Method name. */
        private final String methodName;

        /** Parameter type IDs. */
        private final int[] paramTypeIds;

        /** Hash code. */
        private final int hash;

        /**
         * @param cls Class.
         * @param methodName Method name.
         * @param paramTypeIds Parameter type ids.
         */
        private MethodDescriptor(Class<?> cls, String methodName, int[] paramTypeIds) {
            assert cls != null;
            assert methodName != null;
            assert paramTypeIds != null;

            this.cls = cls;
            this.methodName = methodName;
            this.paramTypeIds = paramTypeIds;

            // Precalculate hash in constructor, since we need it for all objects of this class.
            hash = 31 * ((31 * cls.hashCode()) + methodName.hashCode()) + Arrays.hashCode(paramTypeIds);
        }

        /**
         * @param binary Ignite binary.
         * @param method Method.
         */
        private static MethodDescriptor forMethod(IgniteBinary binary, Method method) {
            Class<?>[] paramTypes = method.getParameterTypes();

            int[] paramTypeIds = new int[paramTypes.length];

            for (int i = 0; i < paramTypes.length; i++)
                paramTypeIds[i] = binary.typeId(paramTypes[i].getName());

            return new MethodDescriptor(method.getDeclaringClass(), method.getName(), paramTypeIds);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MethodDescriptor that = (MethodDescriptor)o;

            return cls.equals(that.cls) && methodName.equals(that.methodName)
                && Arrays.equals(paramTypeIds, that.paramTypeIds);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MethodDescriptor.class, this, "paramTypeIds", paramTypeIds);
        }
    }
}
