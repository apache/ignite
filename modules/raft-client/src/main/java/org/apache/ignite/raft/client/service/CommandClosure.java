/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.service;

import java.io.Serializable;
import org.apache.ignite.raft.client.Command;
import org.jetbrains.annotations.Nullable;

/**
 * A closure to notify about a command processing outcome.
 *
 * @param <R> Command type.
 * @see RaftGroupListener
 */
public interface CommandClosure<R extends Command> {
    /**
     * @return The command.
     */
    R command();

    /**
     * Must be called after a command has been processed normally.
     *
     * @param res Execution result.
     */
    void result(@Nullable Serializable res);
}
