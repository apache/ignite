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
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Resolve-and-dispatch entry points for {@link MessageMarshaller}: each looks up the marshaller registered for the
 * message's direct type in {@code kctx.messageFactory()} and delegates to it, or skips when none is registered.
 */
public final class MessageMarshalling {
    /** */
    private MessageMarshalling() {
        // No-op.
    }

    /**
     * Marshals {@code msg} through its registered marshaller; a no-op when none is registered.
     *
     * @param msg Message to marshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void marshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = resolve(kctx, msg);

        if (m != null)
            m.marshal(msg, kctx, cacheObjCtx);
    }

    /**
     * Unmarshals {@code msg} through its registered marshaller with full cache context; a no-op when none is registered.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader for unmarshalling.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx,
        ClassLoader clsLdr) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, true)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> m = resolve(kctx, msg);

        if (m != null)
            m.unmarshal(msg, kctx, cacheObjCtx, clsLdr);
    }

    /**
     * Cache-free {@code unmarshal} through the registered marshaller; a no-op when none is registered.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, false)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> m = resolve(kctx, msg);

        if (m != null)
            m.unmarshal(msg, kctx);
    }

    /**
     * Unmarshals only {@code @NioField} fields on the NIO thread through the registered marshaller; a no-op when none
     * is registered.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshaller<M> m = resolve(kctx, msg);

        if (m != null)
            m.unmarshalNio(msg, kctx);
    }

    /** @return the marshaller registered for {@code msg}'s direct type, or {@code null} if none. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> @Nullable MessageMarshaller<M> resolve(GridKernalContext kctx, M msg) {
        return (MessageMarshaller<M>)((IgniteMessageFactory)kctx.messageFactory()).marshaller(msg.directType());
    }
}
