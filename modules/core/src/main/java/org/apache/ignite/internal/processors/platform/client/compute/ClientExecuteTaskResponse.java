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

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Execute task response.
 */
public class ClientExecuteTaskResponse extends ClientResponse {
    /** */
    private final ClientComputeTask task;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param task Compute task.
     */
    public ClientExecuteTaskResponse(long reqId, ClientComputeTask task) {
        super(reqId);

        this.task = task;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeLong(task.taskId());
    }

    /** {@inheritDoc} */
    @Override public void onSent() {
        task.onResponseSent();
    }
}
