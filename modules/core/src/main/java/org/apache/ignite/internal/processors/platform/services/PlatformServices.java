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

package org.apache.ignite.internal.processors.platform.services;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterNodeFilterImpl;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetService;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetServiceImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformWriterBiClosure;
import org.apache.ignite.internal.processors.platform.utils.PlatformWriterClosure;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Interop services.
 */
@SuppressWarnings({"UnusedDeclaration"})
public class PlatformServices extends PlatformAbstractTarget {
    /** */
    private static final int OP_DOTNET_DEPLOY = 1;

    /** */
    private static final int OP_DOTNET_DEPLOY_MULTIPLE = 2;

    /** */
    private static final int OP_DOTNET_SERVICES = 3;

    /** */
    private static final int OP_INVOKE = 4;

    /** */
    private static final int OP_DESCRIPTORS = 5;

    /** */
    private static final int OP_WITH_ASYNC = 6;

    /** */
    private static final int OP_WITH_SERVER_KEEP_BINARY = 7;

    /** */
    private static final int OP_SERVICE_PROXY = 8;

    /** */
    private static final int OP_CANCEL = 9;

    /** */
    private static final int OP_CANCEL_ALL = 10;

    /** */
    private static final int OP_DOTNET_DEPLOY_ASYNC = 11;

    /** */
    private static final int OP_DOTNET_DEPLOY_MULTIPLE_ASYNC = 12;

    /** */
    private static final int OP_CANCEL_ASYNC = 13;

    /** */
    private static final int OP_CANCEL_ALL_ASYNC = 14;

    /** */
    private static final int OP_DOTNET_DEPLOY_ALL = 15;

    /** */
    private static final int OP_DOTNET_DEPLOY_ALL_ASYNC = 16;

    /** */
    private static final byte PLATFORM_JAVA = 0;

    /** */
    private static final byte PLATFORM_DOTNET = 1;

    /** */
    private static final CopyOnWriteConcurrentMap<T3<Class, String, Integer>, Method> SVC_METHODS
        = new CopyOnWriteConcurrentMap<>();

    /** Future result writer. */
    private static final PlatformFutureUtils.Writer RESULT_WRITER = new ServiceDeploymentResultWriter();

    /** */
    private final IgniteServices services;

    /** Server keep binary flag. */
    private final boolean srvKeepBinary;

    /**
     * Ctor.
     *
     * @param platformCtx Context.
     * @param services Services facade.
     * @param srvKeepBinary Server keep binary flag.
     */
    public PlatformServices(PlatformContext platformCtx, IgniteServices services, boolean srvKeepBinary) {
        super(platformCtx);

        assert services != null;

        this.services = services;
        this.srvKeepBinary = srvKeepBinary;
    }

    /**
     * Finds a service descriptor by name.
     *
     * @param name Service name.
     * @return Descriptor or null.
     */
    private ServiceDescriptor findDescriptor(String name) {
        for (ServiceDescriptor d : services.serviceDescriptors())
            if (d.name().equals(name))
                return d;

        return null;
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_DOTNET_DEPLOY_ASYNC: {
                readAndListenFuture(reader, dotnetDeployAsync(reader, services), RESULT_WRITER);

                return TRUE;
            }

            case OP_DOTNET_DEPLOY_MULTIPLE_ASYNC: {
                readAndListenFuture(reader, dotnetDeployMultipleAsync(reader), RESULT_WRITER);

                return TRUE;
            }

            case OP_CANCEL: {
                services.cancel(reader.readString());

                return TRUE;
            }

            case OP_CANCEL_ASYNC: {
                readAndListenFuture(reader, services.cancelAsync(reader.readString()));

                return TRUE;
            }

            case OP_CANCEL_ALL_ASYNC: {
                readAndListenFuture(reader, services.cancelAllAsync());

                return TRUE;
            }

            case OP_DOTNET_DEPLOY_ALL_ASYNC: {
                readAndListenFuture(reader, dotnetDeployAllAsync(reader, services), RESULT_WRITER);

                return TRUE;
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_DOTNET_SERVICES: {
                Collection<Service> svcs = services.services(reader.readString());

                PlatformUtils.writeNullableCollection(writer, svcs,
                    new PlatformWriterClosure<Service>() {
                        @Override public void write(BinaryRawWriterEx writer, Service svc) {
                            writer.writeLong(((PlatformService) svc).pointer());
                        }
                    },
                    new IgnitePredicate<Service>() {
                        @Override public boolean apply(Service svc) {
                            return svc instanceof PlatformDotNetService;
                        }
                    }
                );

                return;
            }

            case OP_DOTNET_DEPLOY: {
                try {
                    dotnetDeploy(reader, services);
                    writeDeploymentResult(writer, null);
                }
                catch (Exception e) {
                    writeDeploymentResult(writer, e);
                }

                return;
            }

            case OP_DOTNET_DEPLOY_MULTIPLE: {
                try {
                    dotnetDeployMultiple(reader);

                    writeDeploymentResult(writer, null);
                }
                catch (Exception e) {
                    writeDeploymentResult(writer, e);
                }

                return;
            }

            case OP_DOTNET_DEPLOY_ALL: {
                try {
                    dotnetDeployAll(reader, services);

                    writeDeploymentResult(writer, null);
                }
                catch (Exception e) {
                    writeDeploymentResult(writer, e);
                }

                return;
            }

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(int type, PlatformTarget arg,
        BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_INVOKE: {
                assert arg != null;
                assert arg instanceof ServiceProxyHolder;

                String mthdName = reader.readString();

                Object[] args;

                if (reader.readBoolean()) {
                    args = new Object[reader.readInt()];

                    for (int i = 0; i < args.length; i++)
                        args[i] = reader.readObjectDetached();
                }
                else
                    args = null;

                try {
                    Object result = ((ServiceProxyHolder)arg).invoke(mthdName, srvKeepBinary, args);

                    PlatformUtils.writeInvocationResult(writer, result, null);
                }
                catch (Exception e) {
                    PlatformUtils.writeInvocationResult(writer, null, e);
                }

                return null;
            }
        }

        return super.processInObjectStreamOutObjectStream(type, arg, reader, writer);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_DESCRIPTORS: {
                Collection<ServiceDescriptor> descs = services.serviceDescriptors();

                PlatformUtils.writeCollection(writer, descs, new PlatformWriterClosure<ServiceDescriptor>() {
                    @Override public void write(BinaryRawWriterEx writer, ServiceDescriptor d) {
                        writer.writeString(d.name());
                        writer.writeString(d.cacheName());
                        writer.writeInt(d.maxPerNodeCount());
                        writer.writeInt(d.totalCount());
                        writer.writeUuid(d.originNodeId());
                        writer.writeObject(d.affinityKey());

                        // Write platform. There are only 2 platforms now.
                        byte platform = d.serviceClass().equals(PlatformDotNetServiceImpl.class)
                            ? PLATFORM_DOTNET : PLATFORM_JAVA;
                        writer.writeByte(platform);

                        Map<UUID, Integer> top = d.topologySnapshot();

                        PlatformUtils.writeMap(writer, top, new PlatformWriterBiClosure<UUID, Integer>() {
                            @Override public void write(BinaryRawWriterEx writer, UUID key, Integer val) {
                                writer.writeUuid(key);
                                writer.writeInt(val);
                            }
                        });
                    }
                });

                return;
            }

            default:
                super.processOutStream(type, writer);
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_ASYNC:
                if (services.isAsync())
                    return this;

                return new PlatformServices(platformCtx, services.withAsync(), srvKeepBinary);

            case OP_WITH_SERVER_KEEP_BINARY:
                return srvKeepBinary ? this : new PlatformServices(platformCtx, services, true);
        }

        return super.processOutObject(type);
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_CANCEL_ALL:
                services.cancelAll();

                return TRUE;
        }

        return super.processInLongOutLong(type, val);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_SERVICE_PROXY: {
                String name = reader.readString();
                boolean sticky = reader.readBoolean();

                ServiceDescriptor d = findDescriptor(name);

                if (d == null)
                    throw new IgniteException("Failed to find deployed service: " + name);

                Object proxy = PlatformService.class.isAssignableFrom(d.serviceClass())
                    ? services.serviceProxy(name, PlatformService.class, sticky)
                    : new GridServiceProxy<>(services.clusterGroup(), name, Service.class, sticky, 0,
                        platformCtx.kernalContext());

                return new ServiceProxyHolder(proxy, d.serviceClass(), platformContext());
            }
        }
        return super.processInStreamOutObject(type, reader);
    }

    /**
     * Deploys multiple dotnet services.
     *
     * @param reader Binary reader.
     */
    private void dotnetDeployMultiple(BinaryRawReaderEx reader) {
        String name = reader.readString();
        Object svc = reader.readObjectDetached();
        int totalCnt = reader.readInt();
        int maxPerNodeCnt = reader.readInt();

        services.deployMultiple(name, new PlatformDotNetServiceImpl(svc, platformCtx, srvKeepBinary),
                totalCnt, maxPerNodeCnt);
    }

    /**
     * Asynchronously deploys multiple dotnet services.
     *
     * @param reader Binary reader.
     * @return Future of the operation.
     */
    private IgniteFuture<Void> dotnetDeployMultipleAsync(BinaryRawReaderEx reader) {
        String name = reader.readString();
        Object svc = reader.readObjectDetached();
        int totalCnt = reader.readInt();
        int maxPerNodeCnt = reader.readInt();

        return services.deployMultipleAsync(name, new PlatformDotNetServiceImpl(svc, platformCtx, srvKeepBinary),
            totalCnt, maxPerNodeCnt);
    }

    /**
     * Deploys dotnet service.
     *
     * @param reader Binary reader.
     * @param services Services.
     */
    private void dotnetDeploy(BinaryRawReaderEx reader, IgniteServices services) {
        ServiceConfiguration cfg = dotnetConfiguration(reader);

        services.deploy(cfg);
    }

    /**
     * Deploys dotnet service asynchronously.
     *
     * @param reader Binary reader.
     * @param services Services.
     * @return Future of the operation.
     */
    private IgniteFuture<Void> dotnetDeployAsync(BinaryRawReaderEx reader, IgniteServices services) {
        ServiceConfiguration cfg = dotnetConfiguration(reader);

        return services.deployAsync(cfg);
    }

    /**
     * Deploys a collection of dotnet services.
     *
     * @param reader Binary reader.
     * @param services Services.
     */
    private void dotnetDeployAll(BinaryRawReaderEx reader, IgniteServices services) {
        Collection<ServiceConfiguration> cfgs = dotnetConfigurations(reader);

        services.deployAll(cfgs);
    }

    /**
     * Deploys a collection of dotnet services asynchronously.
     *
     * @param reader Binary reader.
     * @param services Services.
     * @return Future of the operation.
     */
    private IgniteFuture<Void> dotnetDeployAllAsync(BinaryRawReaderEx reader, IgniteServices services) {
        Collection<ServiceConfiguration> cfgs = dotnetConfigurations(reader);

        return services.deployAllAsync(cfgs);
    }

    /**
     * Read the dotnet service configuration.
     *
     * @param reader Binary reader,
     * @return Service configuration.
     */
    @NotNull private ServiceConfiguration dotnetConfiguration(BinaryRawReaderEx reader) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(reader.readString());
        cfg.setService(new PlatformDotNetServiceImpl(reader.readObjectDetached(), platformCtx, srvKeepBinary));
        cfg.setTotalCount(reader.readInt());
        cfg.setMaxPerNodeCount(reader.readInt());
        cfg.setCacheName(reader.readString());
        cfg.setAffinityKey(reader.readObjectDetached());

        Object filter = reader.readObjectDetached();

        if (filter != null)
            cfg.setNodeFilter(platformCtx.createClusterNodeFilter(filter));

        return cfg;
    }

    /**
     * Reads the collection of dotnet service configurations.
     *
     * @param reader Binary reader,
     * @return Service configuration.
     */
    @NotNull private Collection<ServiceConfiguration> dotnetConfigurations(BinaryRawReaderEx reader) {
        int numServices = reader.readInt();

        List<ServiceConfiguration> cfgs = new ArrayList<>(numServices);

        for (int i = 0; i < numServices; i++) {
            cfgs.add(dotnetConfiguration(reader));
        }

        return cfgs;
    }

    /**
     * Proxy holder.
     */
    @SuppressWarnings("unchecked")
    private static class ServiceProxyHolder extends PlatformAbstractTarget {
        /** */
        private final Object proxy;

        /** */
        private final Class serviceClass;

        /** */
        private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS = new HashMap<>();

        /*
          Class initializer.
         */
        static  {
            PRIMITIVES_TO_WRAPPERS.put(boolean.class, Boolean.class);
            PRIMITIVES_TO_WRAPPERS.put(byte.class, Byte.class);
            PRIMITIVES_TO_WRAPPERS.put(char.class, Character.class);
            PRIMITIVES_TO_WRAPPERS.put(double.class, Double.class);
            PRIMITIVES_TO_WRAPPERS.put(float.class, Float.class);
            PRIMITIVES_TO_WRAPPERS.put(int.class, Integer.class);
            PRIMITIVES_TO_WRAPPERS.put(long.class, Long.class);
            PRIMITIVES_TO_WRAPPERS.put(short.class, Short.class);
        }

        /**
         * Ctor.
         *
         * @param proxy Proxy object.
         * @param clazz Proxy class.
         * @param ctx Platform context.
         */
        private ServiceProxyHolder(Object proxy, Class clazz, PlatformContext ctx) {
            super(ctx);

            assert proxy != null;
            assert clazz != null;

            this.proxy = proxy;
            serviceClass = clazz;
        }

        /**
         * Invokes the proxy.
         *
         * @param mthdName Method name.
         * @param srvKeepBinary Binary flag.
         * @param args Args.
         * @return Invocation result.
         * @throws IgniteCheckedException On error.
         * @throws NoSuchMethodException On error.
         */
        public Object invoke(String mthdName, boolean srvKeepBinary, Object[] args)
            throws IgniteCheckedException, NoSuchMethodException {
            if (proxy instanceof PlatformService)
                return ((PlatformService)proxy).invokeMethod(mthdName, srvKeepBinary, args);
            else {
                assert proxy instanceof GridServiceProxy;

                // Deserialize arguments for Java service when not in binary mode
                if (!srvKeepBinary)
                    args = PlatformUtils.unwrapBinariesInArray(args);

                Method mtd = getMethod(serviceClass, mthdName, args);

                try {
                    return ((GridServiceProxy)proxy).invokeMethod(mtd, args);
                }
                catch (Throwable t) {
                    throw IgniteUtils.cast(t);
                }
            }
        }

        /**
         * Finds a suitable method in a class
         *
         * @param clazz Class.
         * @param mthdName Name.
         * @param args Args.
         * @return Method.
         * @throws NoSuchMethodException On error.
         */
        private static Method getMethod(Class clazz, String mthdName, Object[] args) throws NoSuchMethodException {
            assert clazz != null;
            assert mthdName != null;
            assert args != null;

            T3<Class, String, Integer> cacheKey = new T3<>(clazz, mthdName, args.length);
            Method res = SVC_METHODS.get(cacheKey);

            if (res != null)
                return res;

            Method[] allMethods = clazz.getMethods();

            List<Method> methods = new ArrayList<>(allMethods.length);

            // Filter by name and param count
            for (Method m : allMethods)
                if (m.getName().equals(mthdName) && m.getParameterTypes().length == args.length)
                    methods.add(m);

            if (methods.size() == 1) {
                res = methods.get(0);

                // Update cache only when there is a single method with a given name and arg count.
                SVC_METHODS.put(cacheKey, res);

                return res;
            }

            if (methods.isEmpty())
                throw new NoSuchMethodException("Could not find proxy method '" + mthdName + "' in class " + clazz);

            // Filter by param types
            for (int i = 0; i < methods.size(); i++)
                if (!areMethodArgsCompatible(methods.get(i).getParameterTypes(), args))
                    methods.remove(i--);

            if (methods.size() == 1)
                return methods.get(0);

            if (methods.isEmpty())
                throw new NoSuchMethodException("Could not find proxy method '" + mthdName + "' in class " + clazz);

            throw new NoSuchMethodException("Ambiguous proxy method '" + mthdName + "' in class " + clazz);
        }

        /**
         * Determines whether specified method arguments are compatible with given method parameter definitions.
         *
         * @param argTypes Method arg types.
         * @param args Method args.
         * @return Whether specified args are compatible with argTypes.
         */
        private static boolean areMethodArgsCompatible(Class[] argTypes, Object[] args) {
            for (int i = 0; i < args.length; i++){
                // arg is always of a primitive wrapper class, and argTypes can have actual primitive
                Object arg = args[i];
                Class argType = wrap(argTypes[i]);

                if (arg != null && !argType.isAssignableFrom(arg.getClass()))
                    return false;
            }

            return true;
        }

        /**
         * Gets corresponding wrapper for a primitive type.
         * @param c Class to convert.
         *
         * @return Primitive wrapper, or the same class.
         */
        @SuppressWarnings("unchecked")
        private static Class wrap(Class c) {
            return c.isPrimitive() ? PRIMITIVES_TO_WRAPPERS.get(c) : c;
        }
    }

    /**
     * Concurrent map.
     */
    private static class CopyOnWriteConcurrentMap<K, V> {
        /** */
        private volatile Map<K, V> map = new HashMap<>();

        /**
         * Gets a value.
         *
         * @param key Key.
         * @return Value.
         */
        public V get(K key) {
            return map.get(key);
        }

        /**
         * Puts a value.
         *
         * @param key Key.
         * @param val Value.
         */
        public void put(K key, V val) {
            synchronized (this){
                if (map.containsKey(key))
                    return;

                Map<K, V> map0 = new HashMap<>(map);

                map0.put(key, val);

                map = map0;
            }
        }
    }

    /**
     * Writes an EventBase.
     */
    private static class ServiceDeploymentResultWriter implements PlatformFutureUtils.Writer {
        /** <inheritDoc /> */
        @Override public void write(BinaryRawWriterEx writer, Object obj, Throwable err) {
            writeDeploymentResult(writer, err);
        }

        /** <inheritDoc /> */
        @Override public boolean canWrite(Object obj, Throwable err) {
            return true;
        }
    }

    /**
     * Writes a service deployment result for dotnet code.
     *
     * @param writer Writer.
     * @param err Error.
      */
    private static void writeDeploymentResult(BinaryRawWriterEx writer, Throwable err) {
        PlatformUtils.writeInvocationResult(writer, null, err);

        Collection<ServiceConfiguration> failedCfgs = null;

        if (err instanceof ServiceDeploymentException)
            failedCfgs = ((ServiceDeploymentException)err).getFailedConfigurations();

        // write a collection of failed service configurations
        PlatformUtils.writeNullableCollection(writer, failedCfgs, new PlatformWriterClosure<ServiceConfiguration>() {
            @Override public void write(BinaryRawWriterEx writer, ServiceConfiguration svcCfg) {
                writeFailedConfiguration(writer, svcCfg);
            }
        });
    }

    /**
     * Writes a failed service configuration for dotnet code.
     *
     * @param w Writer
     * @param svcCfg Service configuration
     */
    private static void writeFailedConfiguration(BinaryRawWriterEx w, ServiceConfiguration svcCfg) {
        Object dotnetSvc = null;
        Object dotnetFilter = null;
        w.writeString(svcCfg.getName());
        if (svcCfg.getService() instanceof PlatformDotNetServiceImpl)
            dotnetSvc = ((PlatformDotNetServiceImpl)svcCfg.getService()).getInternalService();

        w.writeObjectDetached(dotnetSvc);
        w.writeInt(svcCfg.getTotalCount());
        w.writeInt(svcCfg.getMaxPerNodeCount());
        w.writeString(svcCfg.getCacheName());
        w.writeObjectDetached(svcCfg.getAffinityKey());

        if (svcCfg.getNodeFilter() instanceof PlatformClusterNodeFilterImpl)
            dotnetFilter = ((PlatformClusterNodeFilterImpl)svcCfg.getNodeFilter()).getInternalPredicate();
        w.writeObjectDetached(dotnetFilter);
    }
}
