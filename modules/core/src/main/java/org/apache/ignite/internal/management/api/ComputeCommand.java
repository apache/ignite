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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 * Command that executed with some compute task.
 */
public interface ComputeCommand<A extends IgniteDataTransferObject, R> extends Command<A, R> {
    /** @return Task class. */
    public Class<? extends ComputeTask<VisorTaskArgument<A>, R>> taskClass();

    /**
     * @param nodes Live nodes. Key is node ID, Boolean is client flag, Object is consistent id, Long is node order.
     * @param arg Argument.
     * @return nodes to execute command on, {@code null} means default node must be used.
     */
    public default @Nullable Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, A arg) {
        return null;
    }
}
