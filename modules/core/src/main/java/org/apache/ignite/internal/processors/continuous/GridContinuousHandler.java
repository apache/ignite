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

package org.apache.ignite.internal.processors.continuous;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Continuous routine handler.
 */
@SuppressWarnings("PublicInnerClass")
public interface GridContinuousHandler extends Externalizable, Cloneable {
    /**
     * Listener registration status.
     */
    public enum RegisterStatus {
        /** */
        REGISTERED,

        /** */
        NOT_REGISTERED,

        /** */
        DELAYED
    }

    /**
     * Registers listener.
     *
     * @param nodeId ID of the node that started routine.
     * @param routineId Routine ID.
     * @param ctx Kernal context.
     * @return Whether listener was actually registered.
     * @throws IgniteCheckedException In case of error.
     */
    public RegisterStatus register(UUID nodeId, UUID routineId, GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * Callback called after listener is registered and acknowledgement is sent.
     *
     * @param routineId Routine ID.
     * @param ctx Kernal context.
     */
    public void onListenerRegistered(UUID routineId, GridKernalContext ctx);

    /**
     * Unregisters listener.
     *
     * @param routineId Routine ID.
     * @param ctx Kernal context.
     */
    public void unregister(UUID routineId, GridKernalContext ctx);

    /**
     * Notifies local callback.
     *
     * @param nodeId ID of the node where notification came from.
     * @param routineId Routine ID.
     * @param objs Notification objects.
     * @param ctx Kernal context.
     */
    public void notifyCallback(UUID nodeId, UUID routineId, Collection<?> objs, GridKernalContext ctx);

    /**
     * Deploys and marshals inner objects (called only if peer deployment is enabled).
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * Unmarshals inner objects (called only if peer deployment is enabled).
     *
     * @param nodeId Sender node ID.
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * @return Topic for ordered notifications. If {@code null}, notifications
     * will be sent in non-ordered messages.
     */
    @Nullable public Object orderedTopic();

    /**
     * Clones this handler.
     *
     * @return Clone of this handler.
     */
    public GridContinuousHandler clone();

    /**
     * @return {@code True} if for events.
     */
    public boolean isForEvents();

    /**
     * @return {@code True} if for messaging.
     */
    public boolean isForMessaging();

    /**
     * @return {@code True} if for continuous queries.
     */
    public boolean isForQuery();

    /**
     * @return Cache name if this is a continuous query handler.
     */
    public String cacheName();
}
