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

package org.apache.ignite.internal.processors.platform.client.classpath;

import java.util.UUID;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.classpath.IgniteClassPath;
import org.apache.ignite.internal.processors.odbc.ClientListenerInternalRequest;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Must be used only from control.sh command.
 * Writes part of {@link IgniteClassPath} file.
 *
 * @see ClientListenerInternalRequest
 * @see IgniteClassPath
 */
public class ClientClassPathFileUploadRequest extends ClientRequest implements ClientListenerInternalRequest {
    /** Upload node ID. */
    private final UUID uploadNodeId;

    /** ClassPath ID. */
    private final UUID icpId;

    /** File name. */
    private final String name;

    /** Offset to write data to. */
    private final long offset;

    /** Batch. */
    private final byte[] batch;

    /**
     * Creates the file upload request.
     *
     * @param reader Reader.
     */
    public ClientClassPathFileUploadRequest(BinaryRawReader reader) {
        super(reader);

        uploadNodeId = reader.readUuid();
        icpId = reader.readUuid();
        name = reader.readString();
        offset = reader.readLong();
        batch = reader.readByteArray();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        UUID locNodeId = ctx.kernalContext().localNodeId();

        if (!uploadNodeId.equals(locNodeId))
            throw new IllegalStateException("Wrong node [uploadNode=" + uploadNodeId + ", localNode=" + locNodeId + ']');

        ctx.kernalContext().classPath().writeFilePartFromClient(icpId, name, offset, batch);

        return new ClientResponse(requestId());
    }
}
