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
    public ClientListenerResponse handle(ClientListenerRequest req);

    /**
     * Handle exception.
     *
     * @param e Exception.
     * @param req Request.
     * @return Error response.
     */
    public ClientListenerResponse handleException(Exception e, ClientListenerRequest req);

    /**
     * Write successful handshake response.
     *
     * @param writer Binary writer.
     */
    public void writeHandshake(BinaryWriterExImpl writer);
}
