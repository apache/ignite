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

package org.apache.ignite.internal.table.distributed.command;

import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command deletes entries by the passed keys.
 */
public class DeleteAllCommand extends MultiKeyCommand implements WriteCommand {
    /**
     * Creates a new instance of DeleteAllCommand with the given set of keys to be deleted. The {@code keyRows} should not be {@code null}
     * or empty.
     *
     * @param keyRows   Collection of binary row keys to be deleted.
     * @param timestamp The timestamp.
     *
     * @see TransactionalCommand
     */
    public DeleteAllCommand(@NotNull Collection<BinaryRow> keyRows, @NotNull Timestamp timestamp) {
        super(keyRows, timestamp);
    }
}
