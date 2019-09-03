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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Client listener request handler.
 */
public interface ClientListenerRequestHandler {
    /**
     * Handle request.
     *
     * @param req Request.
     * @return Response.
     */
    ClientListenerResponse handle(ClientListenerRequest req);

    /**
     * Handle exception.
     *
     * @param e Exception.
     * @param req Request.
     * @return Error response.
     */
    ClientListenerResponse handleException(Exception e, ClientListenerRequest req);

    /**
     * Write successful handshake response.
     *
     * @param writer Binary writer.
     */
    void writeHandshake(BinaryWriterExImpl writer);


    /**
     * Checks whether query cancellation is supported within given version of protocol.
     *
     * @return {@code true} if supported, {@code false} otherwise.
     */
    boolean isCancellationSupported();

    /**
     * Detect whether given command is a cancellation command.
     *
     * @param cmdId Command Id
     * @return true if given command is cancellation one, false otherwise;
     */
    boolean isCancellationCommand(int cmdId);

    /**
     * Registers request for futher cancellation if any.
     * @param reqId Request Id.
     * @param cmdType Command Type.
     */
    void registerRequest(long reqId, int cmdType);

    /**
     * Try to unregister request.
     * @param reqId Request Id.
     */
    void unregisterRequest(long reqId);
}
