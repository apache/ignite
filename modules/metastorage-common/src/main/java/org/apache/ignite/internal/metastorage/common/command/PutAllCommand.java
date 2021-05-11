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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Put all command for MetaStorageCommandListener that inserts or updates entries with given keys and given values.
 */
public final class PutAllCommand implements WriteCommand {
    /** The map of keys and corresponding values. Couldn't be {@code null} or empty. */
    @NotNull private final Map<Key, byte[]> vals;

    /**
     * @param vals he map of keys and corresponding values. Couldn't be {@code null} or empty.
     */
    public PutAllCommand(@NotNull Map<Key, byte[]> vals) {
        assert !vals.isEmpty();

        if (vals instanceof Serializable)
            this.vals = vals;
        else
            this.vals = new HashMap<>(vals);
    }

    /**
     * @return The map of keys and corresponding values. Couldn't be  or empty.
     */
    public @NotNull Map<Key, byte[]> values() {
        return vals;
    }
}
