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
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.LogManagerOptions;
import org.apache.ignite.raft.jraft.util.Describer;

/**
 * Log manager.
 */
public interface LogManager extends Lifecycle<LogManagerOptions>, Describer {

    /**
     * Closure to to run in stable state.
     */
    abstract class StableClosure implements Closure {

        protected long firstLogIndex = 0;
        protected List<LogEntry> entries;
        protected int nEntries;

        public StableClosure() {
            // NO-OP
        }

        public long getFirstLogIndex() {
            return this.firstLogIndex;
        }

        public void setFirstLogIndex(final long firstLogIndex) {
            this.firstLogIndex = firstLogIndex;
        }

        public List<LogEntry> getEntries() {
            return this.entries;
        }

        public void setEntries(final List<LogEntry> entries) {
            this.entries = entries;
            if (entries != null) {
                this.nEntries = entries.size();
            }
            else {
                this.nEntries = 0;
            }
        }

        public StableClosure(final List<LogEntry> entries) {
            super();
            setEntries(entries);
        }

    }

    /**
     * Listen on last log index change event, but it's not reliable, the user should not count on this listener to
     * receive all changed events.
     */
    interface LastLogIndexListener {

        /**
         * Called when last log index is changed.
         *
         * @param lastLogIndex last log index
         */
        void onLastLogIndexChanged(final long lastLogIndex);
    }

    /**
     * Adds a last log index listener
     */
    void addLastLogIndexListener(final LastLogIndexListener listener);

    /**
     * Remove the last log index listener.
     */
    void removeLastLogIndexListener(final LastLogIndexListener listener);

    /**
     * Wait the log manager to be shut down.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    void join() throws InterruptedException;

    /**
     * Append log entry vector and wait until it's stable (NOT COMMITTED!)
     *
     * @param entries log entries
     * @param done callback
     */
    void appendEntries(final List<LogEntry> entries, StableClosure done);

    /**
     * Notify the log manager about the latest snapshot, which indicates the logs which can be safely truncated.
     *
     * @param meta snapshot metadata
     */
    void setSnapshot(final SnapshotMeta meta);

    /**
     * We don't delete all the logs before last snapshot to avoid installing snapshot on slow replica. Call this method
     * to drop all the logs before last snapshot immediately.
     */
    void clearBufferedLogs();

    /**
     * Get the log entry at index.
     *
     * @param index the index of log entry
     * @return the log entry with {@code index}
     */
    LogEntry getEntry(final long index);

    /**
     * Get the log term at index.
     *
     * @param index the index of log entry
     * @return the term of log entry
     */
    long getTerm(final long index);

    /**
     * Get the first log index of log
     */
    long getFirstLogIndex();

    /**
     * Get the last log index of log
     */
    long getLastLogIndex();

    /**
     * Get the last log index of log
     *
     * @param isFlush whether to flush from disk.
     */
    long getLastLogIndex(final boolean isFlush);

    /**
     * Return the id the last log.
     *
     * @param isFlush whether to flush all pending task.
     */
    LogId getLastLogId(final boolean isFlush);

    /**
     * Get the configuration at index.
     */
    ConfigurationEntry getConfiguration(final long index);

    /**
     * Check if |current| should be updated to the latest configuration Returns the latest configuration, otherwise
     * null.
     */
    ConfigurationEntry checkAndSetConfiguration(final ConfigurationEntry current);

    /**
     * New log notifier callback.
     */
    interface NewLogCallback {

        /**
         * Called while new log come in.
         *
         * @param arg the waiter pass-in argument
         * @param errorCode error code
         */
        boolean onNewLog(final Object arg, final int errorCode);
    }

    /**
     * Wait until there are more logs since |last_log_index| and |on_new_log| would be called after there are new logs
     * or error occurs, return the waiter id.
     *
     * @param expectedLastLogIndex expected last index of log
     * @param cb callback
     * @param arg the waiter pass-in argument
     */
    long wait(final long expectedLastLogIndex, final NewLogCallback cb, final Object arg);

    /**
     * Remove a waiter.
     *
     * @param id waiter id
     * @return true on success
     */
    boolean removeWaiter(final long id);

    /**
     * Set the applied id, indicating that the log before applied_id (included) can be dropped from memory logs.
     */
    void setAppliedId(final LogId appliedId);

    /**
     * Check log consistency, returns the status
     *
     * @return status
     */
    Status checkConsistency();

}