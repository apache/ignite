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

package org.apache.ignite.internal.management.api;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Command that executed with some compute task.
 */
public interface ComputeCommand<A extends IgniteDataTransferObject, R> extends Command<A, R> {
    /** @return Task class. */
    public Class<? extends VisorMultiNodeTask<A, R, ?>> taskClass();

    /**
     * Prints command result to the user.
     * @param arg Argument.
     * @param res Result.
     * @param printer Implementation specific printer.
     */
    public default void printResult(A arg, R res, Consumer<String> printer) {
        // No-op.
    }

    /**
     * @param nodes Live nodes.
     * @param arg Argument.
     * @return nodes to execute command on, {@code null} means default node must be used.
     */
    public default @Nullable Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, A arg) {
        return null;
    }

    /**
     * @param e Task execution exception to handle.
     * @param printer Implementation specific printer.
     * @return Result if the exception is suppressed.
     */
    default R handleException(Exception e, Consumer<String> printer) throws Exception {
        throw e;
    }
}
