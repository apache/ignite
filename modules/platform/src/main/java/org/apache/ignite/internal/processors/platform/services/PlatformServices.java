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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetService;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetServiceImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformWriterBiClosure;
import org.apache.ignite.internal.processors.platform.utils.PlatformWriterClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;

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
    private static final int OP_DOTNET_INVOKE = 4;

    /** */
    private static final int OP_DESCRIPTORS = 5;

    /** */
    private final IgniteServices services;

    /** Server keep portable flag. */
    private final boolean srvKeepPortable;

    /**
     * Ctor.
     *
     * @param platformCtx Context.
     * @param services Services facade.
     * @param srvKeepPortable Server keep portable flag.
     */
    public PlatformServices(PlatformContext platformCtx, IgniteServices services, boolean srvKeepPortable) {
        super(platformCtx);

        assert services != null;

        this.services = services;
        this.srvKeepPortable = srvKeepPortable;
    }

    /**
     * Gets services with asynchronous mode enabled.
     *
     * @return Services with asynchronous mode enabled.
     */
    public PlatformServices withAsync() {
        if (services.isAsync())
            return this;

        return new PlatformServices(platformCtx, services.withAsync(), srvKeepPortable);
    }

    /**
     * Gets services with server "keep portable" mode enabled.
     *
     * @return Services with server "keep portable" mode enabled.
     */
    public PlatformServices withServerKeepPortable() {
        return srvKeepPortable ? this : new PlatformServices(platformCtx, services, true);
    }

    /**
     * Cancels service deployment.
     *
     * @param name Name of service to cancel.
     */
    public void cancel(String name) {
        services.cancel(name);
    }

    /**
     * Cancels all deployed services.
     */
    public void cancelAll() {
        services.cancelAll();
    }

    /**
     * Gets a remote handle on the service.
     *
     * @param name Service name.
     * @param sticky Whether or not Ignite should always contact the same remote service.
     * @return Either proxy over remote service or local service if it is deployed locally.
     */
    public Object dotNetServiceProxy(String name, boolean sticky) {
        return services.serviceProxy(name, PlatformDotNetService.class, sticky);
    }

    /** {@inheritDoc} */
    @Override protected long processInStreamOutLong(int type, PortableRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_DOTNET_DEPLOY: {
                ServiceConfiguration cfg = new ServiceConfiguration();

                cfg.setName(reader.readString());
                cfg.setService(new PlatformDotNetServiceImpl(reader.readObjectDetached(), platformCtx, srvKeepPortable));
                cfg.setTotalCount(reader.readInt());
                cfg.setMaxPerNodeCount(reader.readInt());
                cfg.setCacheName(reader.readString());
                cfg.setAffinityKey(reader.readObjectDetached());

                Object filter = reader.readObjectDetached();

                if (filter != null)
                    cfg.setNodeFilter(platformCtx.createClusterNodeFilter(filter));

                services.deploy(cfg);

                return TRUE;
            }

            case OP_DOTNET_DEPLOY_MULTIPLE: {
                String name = reader.readString();
                Object svc = reader.readObjectDetached();
                int totalCnt = reader.readInt();
                int maxPerNodeCnt = reader.readInt();

                services.deployMultiple(name, new PlatformDotNetServiceImpl(svc, platformCtx, srvKeepPortable),
                    totalCnt, maxPerNodeCnt);

                return TRUE;
            }

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override protected void processInStreamOutStream(int type, PortableRawReaderEx reader, PortableRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_DOTNET_SERVICES: {
                Collection<Service> svcs = services.services(reader.readString());

                PlatformUtils.writeNullableCollection(writer, svcs,
                    new PlatformWriterClosure<Service>() {
                        @Override public void write(PortableRawWriterEx writer, Service svc) {
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

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override protected void processInObjectStreamOutStream(int type, Object arg, PortableRawReaderEx reader,
        PortableRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_DOTNET_INVOKE: {
                assert arg != null;
                assert arg instanceof PlatformDotNetService;

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
                    Object result = ((PlatformDotNetService)arg).invokeMethod(mthdName, srvKeepPortable, args);

                    PlatformUtils.writeInvocationResult(writer, result, null);
                }
                catch (Exception e) {
                    PlatformUtils.writeInvocationResult(writer, null, e);
                }

                return;
            }

            default:
                super.processInObjectStreamOutStream(type, arg, reader, writer);
        }
    }

    /** {@inheritDoc} */
    @Override protected void processOutStream(int type, PortableRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_DESCRIPTORS: {
                Collection<ServiceDescriptor> descs = services.serviceDescriptors();

                PlatformUtils.writeCollection(writer, descs, new PlatformWriterClosure<ServiceDescriptor>() {
                    @Override public void write(PortableRawWriterEx writer, ServiceDescriptor d) {
                        writer.writeString(d.name());
                        writer.writeString(d.cacheName());
                        writer.writeInt(d.maxPerNodeCount());
                        writer.writeInt(d.totalCount());
                        writer.writeUuid(d.originNodeId());
                        writer.writeObject(d.affinityKey());

                        Map<UUID, Integer> top = d.topologySnapshot();

                        PlatformUtils.writeMap(writer, top, new PlatformWriterBiClosure<UUID, Integer>() {
                            @Override public void write(PortableRawWriterEx writer, UUID key, Integer val) {
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

    /** <inheritDoc /> */
    @Override protected IgniteFuture currentFuture() throws IgniteCheckedException {
        return services.future();
    }
}