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
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Takes a {@link Message} to the state it is written from, and back to the state it is used in. The work itself is in
 * the {@link MessageMarshaller} codegen wrote for the message; this class only finds that companion by the message's
 * direct type and calls it. Writing and reading the bytes is left to {@link MessageSerializer}.
 */
public final class MessageMarshalling {
    /** */
    private MessageMarshalling() {
        // No-op.
    }

    /**
     * Takes {@code msg} to the state it is written from. Called on the user thread before sending.
     *
     * @param msg Message to take to the wire.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void marshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException {
        marshal(factory(kctx), msg, kctx, cacheObjCtx);
    }

    /**
     * Takes {@code msg} to the state it is written from. Callers doing this for many messages resolve
     * {@code msgFactory} once and use this overload.
     *
     * @param msgFactory Message factory to resolve the companions from.
     * @param msg Message to take to the wire.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public static <M extends Message> void marshal(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        MessageMarshaller<M> marshaller = marshaller(msgFactory, msg);

        if (marshaller != null)
            marshaller.marshal(msg, kctx, cacheObjCtx);
    }

    /**
     * Brings {@code msg} back to the state it is used in, with full cache context and class loader.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader to resolve the classes with.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx,
        ClassLoader clsLdr) throws IgniteCheckedException {
        unmarshal(factory(kctx), msg, kctx, cacheObjCtx, clsLdr);
    }

    /**
     * Brings {@code msg} back to the state it is used in. Callers doing this for many messages resolve
     * {@code msgFactory} once and use this overload.
     *
     * @param msgFactory Message factory to resolve the companions from.
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader to resolve the classes with.
     */
    public static <M extends Message> void unmarshal(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, true)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> marshaller = marshaller(msgFactory, msg);

        if (marshaller != null)
            marshaller.unmarshal(msg, kctx, cacheObjCtx, clsLdr);
    }

    /**
     * Brings {@code msg} back without a cache context, using the configuration class loader — the cache-free receive
     * path.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, false)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> marshaller = marshaller(factory(kctx), msg);

        if (marshaller != null)
            marshaller.unmarshal(msg, kctx);
    }

    /**
     * Brings back only the {@code @NioField} fields (routing headers) on the NIO thread. The rest of the message,
     * the other steps included, waits for a worker thread.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageMarshaller<M> marshaller = marshaller(factory(kctx), msg);

        if (marshaller != null)
            marshaller.unmarshalNio(msg, kctx);
    }

    /** @return the message factory of {@code kctx}. */
    private static IgniteMessageFactory factory(GridKernalContext kctx) {
        return (IgniteMessageFactory)kctx.messageFactory();
    }

    /** @return the marshaller registered for {@code msg}'s direct type, or {@code null} if none. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> @Nullable MessageMarshaller<M> marshaller(IgniteMessageFactory msgFactory, M msg) {
        return (MessageMarshaller<M>)msgFactory.marshaller(msg.directType());
    }
}
