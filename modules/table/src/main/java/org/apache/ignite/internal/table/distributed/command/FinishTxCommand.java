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

import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;

/** State machine command to finish a transaction. */
public class FinishTxCommand implements WriteCommand {
    /** The timestamp. */
    private final Timestamp timestamp;

    /** Commit or rollback state. */
    private final boolean finish;

    /**
     * The constructor.
     *
     * @param timestamp The timestamp.
     * @param finish    Commit or rollback state {@code True} to commit.
     */
    public FinishTxCommand(Timestamp timestamp, boolean finish) {
        this.timestamp = timestamp;
        this.finish = finish;
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    public Timestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns commit or rollback state.
     *
     * @return Commit or rollback state.
     */
    public boolean finish() {
        return finish;
    }
}
