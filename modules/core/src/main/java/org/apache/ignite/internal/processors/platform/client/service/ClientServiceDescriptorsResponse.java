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

import java.util.Collection;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.services.ServiceDescriptor;

/** Service descriptors. */
public class ClientServiceDescriptorsResponse extends ClientResponse {
    /** Services descriptors. */
    private final Collection<ServiceDescriptor> svcs;

    /**
     * @param reqId Request id.
     * @param svcs Services descriptors.
     */
    public ClientServiceDescriptorsResponse(long reqId, Collection<ServiceDescriptor> svcs) {
        super(reqId);

        this.svcs = svcs;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryWriterEx writer) {
        super.encode(ctx, writer);

        BinaryUtils.collection(svcs, writer.out(), (out, svc) -> writeDescriptor(writer, svc));
    }

    /** */
    public static void writeDescriptor(BinaryWriterEx writer, ServiceDescriptor svc) {
        writer.writeString(svc.name());
        writer.writeString(svc.serviceClass().getName());
        writer.writeInt(svc.totalCount());
        writer.writeInt(svc.maxPerNodeCount());
        writer.writeString(svc.cacheName());
        writer.writeUuid(svc.originNodeId());
        writer.writeByte(PlatformService.class.isAssignableFrom(svc.serviceClass())
            ? PlatformServices.PLATFORM_DOTNET
            : PlatformServices.PLATFORM_JAVA);
    }
}
