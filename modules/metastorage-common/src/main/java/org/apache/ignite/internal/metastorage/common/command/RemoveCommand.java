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

package org.apache.ignite.internal.metastorage.common.command;

import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Remove command for MetaStorageCommandListener that removes an entry for the given key.
 */
public final class RemoveCommand implements WriteCommand {
    /** The key. Couldn't be {@code null}. */
    @NotNull private final byte[] key;

    /**
     * @param key he key. Couldn't be {@code null}.
     */
    public RemoveCommand(@NotNull ByteArray key) {
        this.key = key.bytes();
    }

    /**
     * @return The key. Couldn't be {@code null}.
     */
    public @NotNull byte[] key() {
        return key;
    }
}
