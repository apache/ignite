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
package org.apache.ignite.raft.jraft.storage;

import java.util.List;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;

/**
 * Log entry storage service.
 */
public interface LogStorage extends Lifecycle<LogStorageOptions>, Storage {
    /**
     * Returns first log index in log.
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     */
    LogEntry getEntry(final long index);

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log
     * id's term.
     *
     * @deprecated
     */
    @Deprecated
    long getTerm(final long index);

    /**
     * Append entries to log.
     */
    boolean appendEntry(final LogEntry entry);

    /**
     * Append entries to log, return append success number.
     */
    int appendEntries(final List<LogEntry> entries);

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will be discarded.
     */
    boolean truncatePrefix(final long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded.
     */
    boolean truncateSuffix(final long lastIndexKept);

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|. This function is called after installing
     * snapshot from leader.
     */
    boolean reset(final long nextLogIndex);
}
