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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.raft.MetaStorageCommandListener;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Remove all command for {@link MetaStorageCommandListener} that removes entries for given keys.
 */
public final class RemoveAllCommand implements WriteCommand {
    /** The keys collection. Couldn't be {@code null}. */
    @NotNull private final Collection<Key> keys;

    /**
     * @param keys The keys collection. Couldn't be {@code null}.
     */
    public RemoveAllCommand(@NotNull Collection<Key> keys) {
        if (keys instanceof Serializable)
            this.keys = keys;
        else
            this.keys = new ArrayList<>(keys);
    }

    /**
     * @return The keys collection. Couldn't be {@code null}.
     */
    public @NotNull Collection<Key> keys() {
        return keys;
    }
}
