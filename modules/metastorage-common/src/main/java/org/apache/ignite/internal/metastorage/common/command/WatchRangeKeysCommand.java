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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Watch command for MetaStorageCommandListener that subscribes on meta storage updates matching the parameters.
 */
public final class WatchRangeKeysCommand implements WriteCommand {
    /** Start key of range (inclusive). Couldn't be {@code null}. */
    @Nullable
    private final byte[] keyFrom;

    /** End key of range (exclusive). Could be {@code null}. */
    @Nullable
    private final byte[] keyTo;

    /** Start revision inclusive. {@code 0} - all revisions. */
    private final long revision;

    /** Id of the node that requests watch. */
    @NotNull
    private final String requesterNodeId;

    /** Id of cursor that is associated with the current command. */
    @NotNull
    private final IgniteUuid cursorId;

    /**
     * @param keyFrom         Start key of range (inclusive).
     * @param keyTo           End key of range (exclusive).
     * @param requesterNodeId Id of the node that requests watch.
     * @param cursorId        Id of cursor that is associated with the current command.*
     */
    public WatchRangeKeysCommand(
            @Nullable ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            @NotNull String requesterNodeId,
            @NotNull IgniteUuid cursorId
    ) {
        this(keyFrom, keyTo, 0L, requesterNodeId, cursorId);
    }

    /**
     * @param keyFrom         Start key of range (inclusive).
     * @param keyTo           End key of range (exclusive).
     * @param revision        Start revision inclusive. {@code 0} - all revisions.
     * @param requesterNodeId Id of the node that requests watch.
     * @param cursorId        Id of cursor that is associated with the current command.
     */
    public WatchRangeKeysCommand(
            @Nullable ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revision,
            @NotNull String requesterNodeId,
            @NotNull IgniteUuid cursorId
    ) {
        this.keyFrom = keyFrom == null ? null : keyFrom.bytes();
        this.keyTo = keyTo == null ? null : keyTo.bytes();
        this.revision = revision;
        this.requesterNodeId = requesterNodeId;
        this.cursorId = cursorId;
    }

    /**
     * @return Start key of range (inclusive). Couldn't be {@code null}.
     */
    public @Nullable byte[] keyFrom() {
        return keyFrom;
    }

    /**
     * @return End key of range (exclusive). Could be {@code null}.
     */
    public @Nullable byte[] keyTo() {
        return keyTo;
    }

    /**
     * @return Start revision inclusive. {@code 0} - all revisions.
     */
    public long revision() {
        return revision;
    }

    /**
     * @return Id of the node that requests range.
     */
    public @NotNull String requesterNodeId() {
        return requesterNodeId;
    }

    /**
     * @return Id of cursor that is associated with the current command.
     */
    public IgniteUuid getCursorId() {
        return cursorId;
    }
}
