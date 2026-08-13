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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Marshalling of the discovery transport. It uses the JDK marshaller, which asks for no type registration at all.
 * <p>
 * A discovery message is marshalled by a thread that has to move discovery forward itself: the ring worker writing
 * it to the socket, a custom event listener marshalling inline, or a node that is not in the ring yet. The binary
 * marshaller would make such a thread wait for a cluster-wide round - a class name mapping, or a new metadata
 * version - and only a discovery thread delivers the answer, so the wait never ends and the ring stops with it.
 * A type the cluster has already accepted marshals fine, but a message cannot rely on that.
 */
public final class DiscoveryMarshalling {
    /** */
    public static <M extends Message> void marshal(M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        MessageMarshalling.marshal(msg, kctx.marshallerContext().jdkMarshaller(), kctx, cacheObjCtx);
    }

    /** */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx.marshallerContext().jdkMarshaller(), kctx);
    }
}
