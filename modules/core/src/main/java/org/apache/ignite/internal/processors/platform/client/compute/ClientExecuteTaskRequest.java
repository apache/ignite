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

package org.apache.ignite.internal.processors.platform.client.compute;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Request to execute compute task.
 */
public class ClientExecuteTaskRequest extends ClientRequest {
    /** Nodes to execute task. */
    private final Set<UUID> nodeIds;

    /** Task name. */
    private final String taskName;

    /** Task argument. */
    private final Object arg;

    /** Task timeout. */
    private final long timeout;

    /** Task flags. */
    private final byte flags;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientExecuteTaskRequest(BinaryRawReader reader) {
        super(reader);

        int cnt = reader.readInt();

        nodeIds = U.newHashSet(cnt);

        for (int i = 0; i < cnt; i++)
            nodeIds.add(reader.readUuid());

        flags = reader.readByte();

        timeout = reader.readLong();

        taskName = reader.readString();

        arg = reader.readObject();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (!ctx.isComputeEnabled()) {
            throw new IgniteClientException(ClientStatus.FUNCTIONALITY_DISABLED,
                "Compute grid functionality is disabled for thin clients on server node. " +
                "To enable it set up ThinClientConfiguration.ComputeEnabled property.");
        }

        ClientComputeTask task = new ClientComputeTask(ctx);

        long taskId = ctx.resources().put(task);

        task.execute(taskId, taskName, arg, nodeIds, flags, timeout);

        return new ClientExecuteTaskResponse(requestId(), task);
    }
}
