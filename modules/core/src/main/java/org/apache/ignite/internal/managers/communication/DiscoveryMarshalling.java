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
 * Entry points of the discovery transport: they name the transport instead of making every caller pick its
 * marshaller. Discovery speaks the JDK marshaller: a discovery message is marshalled on a discovery thread, and a
 * marshaller that registers class names in the cluster would wait there for a discovery round that only this very
 * thread can carry.
 */
public final class DiscoveryMarshalling {
    /** */
    private DiscoveryMarshalling() {
        // No-op.
    }

    /**
     * @param msg Message to marshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void marshal(M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        MessageMarshalling.marshal(msg, kctx.marshallerContext().jdkMarshaller(), kctx, cacheObjCtx);
    }

    /**
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader for unmarshalling.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx.marshallerContext().jdkMarshaller(), kctx, cacheObjCtx, clsLdr);
    }

    /**
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx.marshallerContext().jdkMarshaller(), kctx);
    }
}
