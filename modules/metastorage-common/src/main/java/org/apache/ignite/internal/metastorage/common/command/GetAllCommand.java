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
import org.apache.ignite.raft.client.ReadCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Get all command for {@link MetaStorageCommandListener} that retrieves entries
 * for given keys and the revision upper bound, if latter is present.
 */
public final class GetAllCommand implements ReadCommand {
    /** The collection of keys. */
    @NotNull private final Collection<Key> keys;

    /** The upper bound for entry revisions. Must be positive. */
    @Nullable private Long revUpperBound;

    /**
     * @param keys The collection of keys. Couldn't be {@code null} or empty. Collection elements couldn't be {@code
     * null}.
     */
    public GetAllCommand(@NotNull Collection<Key> keys) {
        assert !keys.isEmpty();

        if (keys instanceof Serializable)
            this.keys = keys;
        else
            this.keys = new ArrayList<>(keys);
    }

    /**
     * @param keys The collection of keys. Couldn't be {@code null} or empty. Collection elements couldn't be {@code
     * null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     */
    public GetAllCommand(@NotNull Collection<Key> keys, @NotNull Long revUpperBound) {
        this(keys);

        assert revUpperBound > 0;

        this.revUpperBound = revUpperBound;
    }

    /**
     * @return The collection of keys.
     */
    public @NotNull Collection<Key> keys() {
        return keys;
    }

    /**
     * @return The upper bound for entry revisions. Must be positive.
     */
    public @Nullable Long revision() {
        return revUpperBound;
    }
}
