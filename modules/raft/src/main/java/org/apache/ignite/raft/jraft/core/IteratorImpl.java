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
package org.apache.ignite.raft.jraft.core;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.error.LogEntryCorruptedException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * The iterator implementation.
 */
public class IteratorImpl {
    private final StateMachine fsm;
    private final LogManager logManager;
    private final List<Closure> closures;
    private final long firstClosureIndex;
    private final NodeOptions options;
    private long currentIndex;
    private final long committedIndex;
    private LogEntry currEntry = new LogEntry(); // blank entry
    private final AtomicLong applyingIndex;
    private RaftException error;

    public IteratorImpl(final StateMachine fsm, final LogManager logManager, final List<Closure> closures,
        final long firstClosureIndex, final long lastAppliedIndex, final long committedIndex,
        final AtomicLong applyingIndex, NodeOptions options) {
        super();
        this.fsm = fsm;
        this.logManager = logManager;
        this.closures = closures;
        this.firstClosureIndex = firstClosureIndex;
        this.currentIndex = lastAppliedIndex;
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        this.options = options;
        next();
    }

    @Override
    public String toString() {
        return "IteratorImpl [fsm=" + this.fsm + ", logManager=" + this.logManager + ", closures=" + this.closures
            + ", firstClosureIndex=" + this.firstClosureIndex + ", currentIndex=" + this.currentIndex
            + ", committedIndex=" + this.committedIndex + ", currEntry=" + this.currEntry + ", applyingIndex="
            + this.applyingIndex + ", error=" + this.error + "]";
    }

    public LogEntry entry() {
        return this.currEntry;
    }

    public RaftException getError() {
        return this.error;
    }

    public boolean isGood() {
        return this.currentIndex <= this.committedIndex && !hasError();
    }

    public boolean hasError() {
        return this.error != null;
    }

    /**
     * Move to next
     */
    public void next() {
        this.currEntry = null; //release current entry
        //get next entry
        if (this.currentIndex <= this.committedIndex) {
            ++this.currentIndex;
            if (this.currentIndex <= this.committedIndex) {
                try {
                    this.currEntry = this.logManager.getEntry(this.currentIndex);
                    if (this.currEntry == null) {
                        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                        getOrCreateError().getStatus().setError(-1,
                            "Fail to get entry at index=%d while committed_index=%d", this.currentIndex,
                            this.committedIndex);
                    }
                }
                catch (final LogEntryCorruptedException e) {
                    getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                    getOrCreateError().getStatus().setError(RaftError.EINVAL, e.getMessage());
                }
                this.applyingIndex.set(this.currentIndex);
            }
        }
    }

    public long getIndex() {
        return this.currentIndex;
    }

    public Closure done() {
        if (this.currentIndex < this.firstClosureIndex) {
            return null;
        }
        return this.closures.get((int) (this.currentIndex - this.firstClosureIndex));
    }

    protected void runTheRestClosureWithError() {
        for (long i = Math.max(this.currentIndex, this.firstClosureIndex); i <= this.committedIndex; i++) {
            final Closure done = this.closures.get((int) (i - this.firstClosureIndex));
            if (done != null) {
                Requires.requireNonNull(this.error, "error");
                Requires.requireNonNull(this.error.getStatus(), "error.status");
                final Status status = this.error.getStatus();
                Utils.runClosureInThread(options.getCommonExecutor(), done, status);
            }
        }
    }

    public void setErrorAndRollback(final long ntail, final Status st) {
        Requires.requireTrue(ntail > 0, "Invalid ntail=" + ntail);
        if (this.currEntry == null || this.currEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            this.currentIndex -= ntail;
        }
        else {
            this.currentIndex -= ntail - 1;
        }
        this.currEntry = null;
        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE);
        getOrCreateError().getStatus().setError(RaftError.ESTATEMACHINE,
            "StateMachine meet critical error when applying one or more tasks since index=%d, %s", this.currentIndex,
            st != null ? st.toString() : "none");

    }

    private RaftException getOrCreateError() {
        if (this.error == null) {
            this.error = new RaftException();
        }
        return this.error;
    }
}
