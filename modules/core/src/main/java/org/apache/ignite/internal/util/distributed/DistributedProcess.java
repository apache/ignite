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
 * Distributed process is a cluster-wide process that accumulates single nodes results to finish itself.
 * <p>
 * The process consists of the following phases:
 * <ol>
 *  <li>Initial request starts process. (Sent via discovery)</li>
 *  <li>Each server node process an initial request and send the single node result to the coordinator. (Sent via communication)</li>
 *  <li>The coordinator processes all single nodes results and finish or cancel process. (Sent via discovery)</li>
 * </ol>
 *
 * @param <I> Init request.
 * @param <S> Single node result.
 * @param <R> Result.
 */
public interface DistributedProcess<I extends Serializable, S extends Serializable, R extends Serializable> {
    /**
     * Executes some action and returns future with the single node result to send to the coordinator.
     *
     * @param req Init request.
     * @return Future for this operation.
     */
    IgniteInternalFuture<S> execute(I req);

    /**
     * Builds the process result to finish it.
     * <p>
     * Called on the coordinator when all single nodes results received.
     *
     * @param map Map of single nodes result.
     * @return Result to finish process.
     */
    R buildResult(Map<UUID, S> map);

    /**
     * Finish process.
     *
     * @param res Result.
     */
    void finish(R res);

    /**
     * Cancel process.
     *
     * @param e Exception.
     */
    void cancel(Exception e);
}
