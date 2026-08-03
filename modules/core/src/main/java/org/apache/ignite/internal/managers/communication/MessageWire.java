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
import org.apache.ignite.internal.CustomWireFormMessage;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Takes a {@link Message} to the state it is written from, and back to the state it is used in. Two steps take part,
 * and this class is the only place that knows both and their order:
 * <ul>
 *     <li>marshalling, done by the generated marshaller — see {@code MessageMarshalling};</li>
 *     <li>the step a {@link CustomWireFormMessage} does itself, which is not marshalling.</li>
 * </ul>
 * The message's own step runs first on the way out and last on the way in, so marshalling always deals with the
 * fields as they go on the wire. Writing and reading the bytes themselves is left to {@link MessageSerializer}.
 */
public final class MessageWire {
    /** */
    private MessageWire() {
        // No-op.
    }

    /**
     * Takes {@code msg} to the state it is written from. Called on the user thread before sending.
     *
     * @param msg Message to take to the wire.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void toWire(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException {
        wireForm(msg, true);

        MessageMarshalling.marshal(msg, kctx, cacheObjCtx);
    }

    /**
     * Takes {@code msg} to the state it is written from. Callers doing this for many messages resolve
     * {@code msgFactory} once and use this overload.
     *
     * @param msgFactory Message factory to resolve the marshaller from.
     * @param msg Message to take to the wire.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void toWire(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        wireForm(msg, true);

        MessageMarshalling.marshal(msgFactory, msg, kctx, cacheObjCtx);
    }

    /**
     * Brings {@code msg} back to the state it is used in, with full cache context and class loader.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader to resolve the classes with.
     */
    public static <M extends Message> void fromWire(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx,
        ClassLoader clsLdr) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx, cacheObjCtx, clsLdr);

        wireForm(msg, false);
    }

    /**
     * Brings {@code msg} back to the state it is used in. Callers doing this for many messages resolve
     * {@code msgFactory} once and use this overload.
     *
     * @param msgFactory Message factory to resolve the marshaller from.
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader to resolve the classes with.
     */
    public static <M extends Message> void fromWire(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msgFactory, msg, kctx, cacheObjCtx, clsLdr);

        wireForm(msg, false);
    }

    /**
     * Brings {@code msg} back without a cache context, using the configuration class loader — the cache-free receive
     * path.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void fromWire(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshal(msg, kctx);

        wireForm(msg, false);
    }

    /**
     * Brings back only the {@code @NioField} fields (routing headers) on the NIO thread. The rest of the message,
     * the message's own step included, waits for a worker thread.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void fromWireNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshalling.unmarshalNio(msg, kctx);
    }

    /**
     * Runs the step the message does itself; a no-op for a message without one.
     *
     * @param msg Message to run the step of.
     * @param out {@code true} on the way out, {@code false} on the way in.
     */
    @SuppressWarnings("deprecation")
    private static void wireForm(Message msg, boolean out) {
        if (!(msg instanceof CustomWireFormMessage))
            return;

        if (out)
            ((CustomWireFormMessage)msg).toWireForm();
        else
            ((CustomWireFormMessage)msg).fromWireForm();
    }
}
