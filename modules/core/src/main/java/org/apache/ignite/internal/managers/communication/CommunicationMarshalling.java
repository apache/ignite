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
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Marshalling of the Communication transport. It uses the schema-aware marshaller, {@link BinaryMarshaller}, which
 * writes a type once and refers to it by id afterwards, so a type unknown to the cluster has to be registered first.
 */
public final class CommunicationMarshalling {
    /** */
    public static <M extends Message> void marshal(M msg,
        GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        MessageMarshalling.marshal(msg, kctx.marshaller(), kctx, cacheObjCtx);
    }

    /** */
    public static <M extends Message> void unmarshal(M msg,
        GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx,
        ClassLoader clsLdr) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx.marshaller(), kctx, cacheObjCtx, clsLdr);
    }

    /** */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx.marshaller(), kctx);
    }

    /** */
    public static <M extends Message> void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshalNio(msg, kctx.marshaller(), kctx);
    }
}
