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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class SnapshotOperationResponse implements Message {
    /** Results of single-node handlers execution. */
    @Order(value = 0, method = "handlerResults")
    private Map<String, SnapshotHandlerResult<Message>> hndResults;

    /** Default constructor for {@link GridIoMessageFactory}. */
    public SnapshotOperationResponse() {
        // No-op.
    }

    /** @param hndResults Results of single-node handlers execution.  */
    public SnapshotOperationResponse(Map<String, SnapshotHandlerResult<Message>> hndResults) {
        this.hndResults = hndResults;
    }

    /** @return Results of single-node handlers execution. */
    public @Nullable Map<String, SnapshotHandlerResult<Message>> handlerResults() {
        return hndResults;
    }

    /** @param hndResults Results of single-node handlers execution. */
    public void handlerResults(Map<String, SnapshotHandlerResult<Message>> hndResults) {
        this.hndResults = hndResults;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 517;
    }
}
