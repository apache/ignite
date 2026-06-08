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

import java.io.IOException;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/** */
public class ClientFileUploadRequest extends ClientRequest {
    /** Upload node ID. */
    private final UUID uploadNodeId;

    /** ClassPath ID. */
    private final UUID icpId;

    /** File name. */
    private final String name;

    /** Offset to write data to. */
    private final long offset;

    /** Bytes count in batch to write. */
    private final int bytesCnt;

    /** Batch. */
    private final byte[] batch;

    /**
     * Creates the file upload request.
     *
     * @param reader Reader.
     */
    public ClientFileUploadRequest(BinaryRawReader reader) {
        super(reader);

        uploadNodeId = reader.readUuid();
        icpId = reader.readUuid();
        name = reader.readString();
        offset = reader.readLong();
        bytesCnt = reader.readInt();
        batch = reader.readByteArray();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        // TODO: add backward compatibility support.

        UUID locNodeId = ctx.kernalContext().localNodeId();

        if (!uploadNodeId.equals(locNodeId))
            throw new IllegalStateException("Wrong node [uploadNode=" + uploadNodeId + ", localNode=" + locNodeId + ']');

        try {
            ctx.kernalContext().classPath().uploadFilePart(icpId, name, offset, bytesCnt, batch);

            return new ClientResponse(requestId());
        }
        catch (IOException e) {
            return new ClientResponse(requestId(), e.getMessage());
        }
    }
}
