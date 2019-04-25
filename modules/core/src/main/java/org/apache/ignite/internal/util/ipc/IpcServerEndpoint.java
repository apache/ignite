/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.ipc;

import java.io.Closeable;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * IPC server endpoint that is capable for client connections accepting.
 */
public interface IpcServerEndpoint extends Closeable {
    /**
     * Accepts client IPC connection. After client connection is accepted, it can be used
     * for IPC. This method will block until client connects to IPC server endpoint.
     *
     * @return Accepted client connection.
     * @throws IgniteCheckedException If accept failed and the endpoint is not usable anymore.
     */
    public IpcEndpoint accept() throws IgniteCheckedException;

    /**
     * Starts configured endpoint implementation.
     *
     * @throws IgniteCheckedException If failed to start server endpoint.
     */
    public void start() throws IgniteCheckedException;

    /**
     * Closes server IPC. After IPC is closed, no further operations can be performed on this
     * object.
     */
    @Override public void close();

    /**
     * Gets port endpoint is bound to.
     * Endpoints who does not bind to any port should return -1.
     *
     * @return Port number.
     */
    public int getPort();

    /**
     * Gets host endpoint is bound to.
     * Endpoints who does not bind to any port should return {@code null}.
     *
     * @return Host.
     */
    @Nullable public String getHost();

    /**
     * Indicates if this endpoint is a management endpoint.
     *
     * @return {@code true} if it's a management endpoint.
     */
    public boolean isManagement();
}