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
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/**
 *
 */
public interface Command<A extends IgniteDataTransferObject, R, T extends ComputeTask<VisorTaskArgument<A>, R>> {
    /** Command description. */
    public String description();

    /** @return Empty arguments class. */
    public Class<A> args();

    /** @return Task class. */
    public Class<T> task();

    /** */
    public default void printResult(IgniteDataTransferObject arg, Object res, IgniteLogger log) {
        // No-op.
    }

    /**
     * Return {@code true} if the command is experimental or {@code false}
     * otherwise.
     *
     * @return {@code true} if the command is experimental or {@code false}
     *      otherwise.
     */
    default boolean experimental() {
        return false;
    }

    /**
     * Return {@code true} if the command is experimental or {@code false}
     * otherwise.
     *
     * @return {@code true} if the command is experimental or {@code false}
     *      otherwise.
     */
    default boolean confirmable() {
        return false;
    }

    /** */
    default Collection<UUID> nodes(Collection<UUID> nodes, A arg) {
        return Collections.emptyList();
    }
}
