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

package org.apache.ignite.internal.util.distributed;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 * Distributed process action.
 *
 * @see DistributedProcess
 *
 * @param <I> Request type.
 * @param <R> Result type.
 */
public interface DistributedProcessAction<I extends Serializable, R extends Serializable> {
    /**
     * Executes some action and returns future with the single node result to send to the coordinator.
     *
     * @param req Init request.
     * @return Future for this operation.
     */
    IgniteInternalFuture<R> execute(I req);

    /**
     * Called when all single nodes results received.
     *
     * @param res Map of single nodes result.
     * @param err Map of single nodes errors.
     */
    void finish(Map<UUID, R> res, Map<UUID, Exception> err);
}
