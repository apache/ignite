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
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWireForm;
import org.jetbrains.annotations.Nullable;

/**
 * Takes a {@link Message} to the state it is written from, and back to the state it is used in. Two steps take part,
 * and this class is the only place that knows both and their order:
 * <ul>
 *     <li>the step a {@link CustomWireFormMessage} does itself;</li>
 *     <li>{@link MessageMarshaller} — the fields that become bytes;</li>
 *     <li>{@link MessageWireForm} — walking the nested messages and cache objects.</li>
 * </ul>
 * On the way out they run in that order, on the way in in reverse, so each step sees the fields in the shape the
 * next one expects. Writing and reading the bytes themselves is left to {@link MessageSerializer}.
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
        toWire(factory(kctx), msg, kctx, cacheObjCtx);
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
    public static <M extends Message> void toWire(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        ownStep(msg, true);

        MessageMarshaller<M> marshaller = marshaller(msgFactory, msg);

        if (marshaller != null)
            marshaller.marshal(msg, kctx, cacheObjCtx);

        MessageWireForm<M> wireForm = wireForm(msgFactory, msg);

        if (wireForm != null)
            wireForm.toWire(msg, kctx, cacheObjCtx);
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
        fromWire(factory(kctx), msg, kctx, cacheObjCtx, clsLdr);
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
    public static <M extends Message> void fromWire(IgniteMessageFactory msgFactory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, true)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageWireForm<M> wireForm = wireForm(msgFactory, msg);

        if (wireForm != null)
            wireForm.fromWire(msg, kctx, cacheObjCtx, clsLdr);

        MessageMarshaller<M> marshaller = marshaller(msgFactory, msg);

        if (marshaller != null)
            marshaller.unmarshal(msg, kctx, cacheObjCtx, clsLdr);

        ownStep(msg, false);
    }

    /**
     * Brings {@code msg} back without a cache context, using the configuration class loader — the cache-free receive
     * path.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void fromWire(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        assert !MessageUnmarshalOnceCheck.ENABLED || MessageUnmarshalOnceCheck.firstUnmarshal(msg, false)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        IgniteMessageFactory msgFactory = factory(kctx);

        MessageWireForm<M> wireForm = wireForm(msgFactory, msg);

        if (wireForm != null)
            wireForm.fromWire(msg, kctx);

        MessageMarshaller<M> marshaller = marshaller(msgFactory, msg);

        if (marshaller != null)
            marshaller.unmarshal(msg, kctx);

        ownStep(msg, false);
    }

    /**
     * Brings back only the {@code @NioField} fields (routing headers) on the NIO thread. The rest of the message,
     * the other steps included, waits for a worker thread.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    public static <M extends Message> void fromWireNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        MessageWireForm<M> wireForm = wireForm(factory(kctx), msg);

        if (wireForm != null)
            wireForm.fromWireNio(msg, kctx);
    }

    /**
     * Runs the step the message does itself; a no-op for a message without one.
     *
     * @param msg Message to run the step of.
     * @param out {@code true} on the way out, {@code false} on the way in.
     */
    private static void ownStep(Message msg, boolean out) {
        if (!(msg instanceof CustomWireFormMessage))
            return;

        if (out)
            ((CustomWireFormMessage)msg).toWireForm();
        else
            ((CustomWireFormMessage)msg).fromWireForm();
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

    /** @return the wire form registered for {@code msg}'s direct type, or {@code null} if none. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> @Nullable MessageWireForm<M> wireForm(IgniteMessageFactory msgFactory, M msg) {
        return (MessageWireForm<M>)msgFactory.wireForm(msg.directType());
    }
}
