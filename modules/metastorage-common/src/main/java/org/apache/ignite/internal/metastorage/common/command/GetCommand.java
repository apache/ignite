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
import org.apache.ignite.raft.client.ReadCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Get command for MetaStorageCommandListener that retrieves an entry for the given key and the revision upper bound, if
 * latter is present.
 */
public final class GetCommand implements ReadCommand {
    /** Key. */
    @NotNull private final Key key;

    /** The upper bound for entry revisions. Must be positive. */
    @Nullable private Long revUpperBound;

    /**
     * @param key Key. Couldn't be {@code null}.
     */
    public GetCommand(@NotNull Key key) {
        this.key = key;
    }

    /**
     * @param key Key. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     */
    public GetCommand(@NotNull Key key, @NotNull Long revUpperBound) {
        this.key = key;

        assert revUpperBound > 0;

        this.revUpperBound = revUpperBound;
    }

    /**
     * @return Key.
     */
    public @NotNull Key key() {
        return key;
    }

    /**
     * @return The upper bound for entry revisions, or {@code null} if wasn't specified.
     */
    public @Nullable Long revision() {
        return revUpperBound;
    }
}
