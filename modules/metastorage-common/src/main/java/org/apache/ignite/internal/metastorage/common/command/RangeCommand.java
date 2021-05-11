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

import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.raft.MetaStorageCommandListener;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Range command for {@link MetaStorageCommandListener} that retrieves entries for the given
 * key range in lexicographic order. Entries will be filtered out by upper bound of given revision number.
 */
public final class RangeCommand implements WriteCommand {
    /** Start key of range (inclusive). Couldn't be {@code null}. */
    @NotNull private final Key keyFrom;

    /** End key of range (exclusive). Could be {@code null}. */
    @Nullable private final Key keyTo;

    /** The upper bound for entry revision. {@code -1} means latest revision. */
    @NotNull private final Long revUpperBound;

    /**
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo End key of range (exclusive).
     */
    public RangeCommand(@NotNull Key keyFrom, @Nullable Key keyTo) {
        this(keyFrom, keyTo, -1L);
    }

    /**
     * @param keyFrom Start key of range (inclusive).
     * @param keyTo End key of range (exclusive).
     * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
     */
    public RangeCommand(
        @NotNull Key keyFrom,
        @Nullable Key keyTo,
        @NotNull Long revUpperBound
    ) {
        this.keyFrom = keyFrom;
        this.keyTo = keyTo;
        this.revUpperBound = revUpperBound;
    }

    /**
     * @return Start key of range (inclusive). Couldn't be {@code null}.
     */
    public @NotNull Key keyFrom() {
        return keyFrom;
    }

    /**
     * @return End key of range (exclusive). Could be {@code null}.
     */
    public @Nullable Key keyTo() {
        return keyTo;
    }

    /**
     * @return The upper bound for entry revision. Means latest revision.
     */
    public @NotNull Long revUpperBound() {
        return revUpperBound;
    }
}
