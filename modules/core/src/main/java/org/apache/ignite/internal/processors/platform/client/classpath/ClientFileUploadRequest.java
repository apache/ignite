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
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridClosureCallMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;

/** */
public class ClientFileUploadRequest extends ClientRequest {
    /** */
    private final FileUploadPartCallable data;

    /**
     * Creates the file upload request.
     *
     * @param reader Reader.
     */
    public ClientFileUploadRequest(BinaryRawReader reader) {
        super(reader);

        data = new FileUploadPartCallable(
            reader.readUuid(),
            reader.readUuid(),
            reader.readString(),
            reader.readLong(),
            reader.readInt(),
            reader.readByteArray()
        );
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        // TODO: add backward compatibility support.

        try {
            // Forward request to upload node.
            if (!ctx.kernalContext().localNodeId().equals(data.uploadNodeId)) {
                ClusterNode uploadNode = ctx.kernalContext().cluster().get().node(data.uploadNodeId);

                if (uploadNode == null) {
                    throw new IgniteException("Upload node not found [localNode=" + ctx.kernalContext().localNodeId() +
                        ", uploadNode=" + data.uploadNodeId + ", icp=" + data.name + ']');
                }

                ctx.kernalContext().closure().callAsync(
                    GridClosureCallMode.BALANCE,
                    data,
                    options(Collections.singletonList(uploadNode)).withFailoverDisabled()
                ).get();

                return new ClientResponse(requestId());
            }

            ctx.kernalContext().classPath().uploadFilePart(data.icpId, data.name, data.offset, data.bytesCnt, data.batch);

            return new ClientResponse(requestId());
        }
        catch (IgniteCheckedException | IOException e) {
            return new ClientResponse(requestId(), e.getMessage());
        }
    }

    /** */
    private static class FileUploadPartCallable implements IgniteCallable<Void> {
        /** */
        private static final long serialVersionUID = 0L;

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

        /** */
        public FileUploadPartCallable(UUID uploadNodeId, UUID icpId, String name, long offset, int bytesCnt, byte[] batch) {
            this.uploadNodeId = uploadNodeId;
            this.icpId = icpId;
            this.name = name;
            this.offset = offset;
            this.bytesCnt = bytesCnt;
            this.batch = batch;
        }

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            UUID locNodeId = ignite.localNode().id();

            assert uploadNodeId.equals(locNodeId)
                : "Forwarded to wrong node [uploadNode=" + uploadNodeId + ", localNode=" + locNodeId + ']';

            ignite.context().classPath().uploadFilePart(icpId, name, offset, bytesCnt, batch);

            return null;
        }
    }
}
