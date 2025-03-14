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

package org.apache.ignite.internal.processors.igfs;

import java.io.DataInput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.igfs.common.IgfsMessage;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS server message handler. Server component that is plugged in to the server implementation
 * to handle incoming messages asynchronously.
 */
public interface IgfsServerHandler {
    /**
     * Asynchronously handles incoming message.
     *
     * @param ses Client session.
     * @param msg Message to process.
     * @param in Data input. Stream to read from in case if this is a WRITE_BLOCK message.
     * @return Future that will be completed when response is ready or {@code null} if no
     *      response is required.
     */
    @Nullable public IgniteInternalFuture<IgfsMessage> handleAsync(IgfsClientSession ses,
        IgfsMessage msg, DataInput in);

    /**
     * Handles handles client close events.
     *
     * @param ses Session that was closed.
     */
    public void onClosed(IgfsClientSession ses);

    /**
     * Stops handling of incoming requests. No server commands will be handled anymore.
     *
     * @throws IgniteCheckedException If error occurred.
     */
    public void stop() throws IgniteCheckedException;
}