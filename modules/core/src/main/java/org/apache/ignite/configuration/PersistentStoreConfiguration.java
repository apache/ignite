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
package org.apache.ignite.configuration;

import java.io.Serializable;

/**
 * Configures Apache Ignite Persistent store.
 */
public class PersistentStoreConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int DFLT_CHECKPOINT_FREQ = 180000;

    /** Lock default wait time, 10 sec. */
    public static final int DFLT_LOCK_WAIT_TIME = 10 * 1000;

    /** */
    @SuppressWarnings("UnnecessaryBoxing")
    public static final Long DFLT_CHECKPOINT_PAGE_BUFFER_SIZE = new Long(256L * 1024 * 1024);

    /** Default number of checkpoint threads. */
    public static final int DFLT_CHECKPOINT_THREADS = 1;

    /** */
    private static final int DFLT_WAL_HISTORY_SIZE = 20;

    /** */
    public static final int DFLT_WAL_SEGMENTS = 10;

    /** */
    private static final int DFLT_WAL_SEGMENT_SIZE = 64 * 1024 * 1024;

    /** */
    private String persistenteStorePath;

    /** Checkpoint frequency. */
    private long checkpointFreq = DFLT_CHECKPOINT_FREQ;

    /** Lock wait time. */
    private int lockWaitTime = DFLT_LOCK_WAIT_TIME;

    /** */
    private Long checkpointPageBufSize = DFLT_CHECKPOINT_PAGE_BUFFER_SIZE;

    /** */
    private int checkpointThreads = DFLT_CHECKPOINT_THREADS;

    /** */
    private int walHistSize = DFLT_WAL_HISTORY_SIZE;

    /** Number of work WAL segments. */
    private int walSegments = DFLT_WAL_SEGMENTS;

    /** Number of WAL segments to keep. */
    private int walSegmentSize = DFLT_WAL_SEGMENT_SIZE;

    /** Write-ahead log persistence path. */
    private String walStorePath;

    /** Write-ahead log archive path. */
    private String walArchivePath;

    /**
     *
     */
    public String getPersistentStorePath() {
        return persistenteStorePath;
    }

    /**
     * @param persistenceStorePath Persistence store path.
     */
    public PersistentStoreConfiguration setPersistentStorePath(String persistenceStorePath) {
        this.persistenteStorePath = persistenceStorePath;

        return this;
    }

    /**
     * Gets checkpoint frequency.
     *
     * @return Checkpoint frequency in milliseconds.
     */
    public long getCheckpointFrequency() {
        return checkpointFreq <= 0 ? DFLT_CHECKPOINT_FREQ : checkpointFreq;
    }

    /**
     * Sets checkpoint frequency. This is a minimal interval at which memory state will be written to a disk
     * storage. If update rate is high, checkpoints can happen more frequently.
     *
     * @param checkpointFreq Checkpoint frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointFrequency(long checkpointFreq) {
        this.checkpointFreq = checkpointFreq;

        return this;
    }

    /**
     * Time out in second, while wait and try get file lock for start persist manager.
     *
     * @return Time for wait.
     */
    public int getLockWaitTime() {
        return lockWaitTime;
    }

    /**
     * Time out in milliseconds, while wait and try get file lock for start persist manager.
     *
     * @param lockWaitTime Lock wait time.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setLockWaitTime(int lockWaitTime) {
        this.lockWaitTime = lockWaitTime;

        return this;
    }



    /**
     * Gets amount of memory allocated for checkpoint temporary buffer. This buffer is used to create temporary
     * copies of pages when checkpoint is in progress.
     *
     * @return Checkpoint page buffer size.
     */
    public Long getCheckpointPageBufferSize() {
        return checkpointPageBufSize;
    }

    /**
     * Sets amount of memory allocated for checkpoint temporary buffer. This buffer is used to create temporary
     * copies of pages when checkpoint is in progress.
     *
     * @param checkpointPageBufSize Checkpoint page buffer size.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointPageBufferSize(long checkpointPageBufSize) {
        this.checkpointPageBufSize = checkpointPageBufSize;

        return this;
    }



    /**
     * Gets number of checkpoint threads to run.
     *
     * @return Number of checkpoint threads.
     */
    public int getCheckpointThreads() {
        return checkpointThreads;
    }

    /**
     * Sets number of checkpoint threads.
     *
     * @param checkpointThreads Number of checkpoint threads.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointThreads(int checkpointThreads) {
        this.checkpointThreads = checkpointThreads;

        return this;
    }

    /**
     * Gets the number checkpoints to keep in WAL history.
     *
     * @return Number of WAL segments to keep after the checkpoint is finished.
     */
    public int getWalHistorySize() {
        return walHistSize <= 0 ? DFLT_WAL_HISTORY_SIZE : walHistSize;
    }

    /**
     * Sets the number of checkpoints to keep in WAL history.
     *
     * @param walHistSize Number of WAL segments to keep after the checkpoint is finished.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalHistorySize(int walHistSize) {
        this.walHistSize = walHistSize;

        return this;
    }

    /**
     * Gets the number of Write Ahead Log segments to work with. Write-ahead log is written over a fixed number
     * of preallocated file segments of fixed size. This parameter sets the number of these segments.
     *
     * @return Number of work WAL segments.
     */
    public int getWalSegments() {
        return walSegments <= 0 ? DFLT_WAL_SEGMENTS : walSegments;
    }

    /**
     * Sets then number of work Write Ahead Log segments.
     *
     * @param walSegments Number of work WAL segments.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegments(int walSegments) {
        this.walSegments = walSegments;

        return this;
    }

    /**
     * @return WAL segment size.
     */
    public int getWalSegmentSize() {
        return walSegmentSize <= 0 ? DFLT_WAL_SEGMENT_SIZE : walSegmentSize;
    }

    /**
     * @param walSegmentSize WAL segment size.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegmentSize(int walSegmentSize) {
        this.walSegmentSize = walSegmentSize;

        return this;
    }

    /**
     * Gets write-ahead log persistence path. If this path is relative, it will be resolved relative to
     * Ignite work directory.
     *
     * @return Write-ahead log persistence path, absolute or relative to Ignite work directory.
     */
    public String getWalStorePath() {
        return walStorePath;
    }

    /**
     * Sets write-ahead log persistence path.
     *
     * @param walStorePath Write-ahead log persistence path, absolute or relative to Ignite work directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalStorePath(String walStorePath) {
        this.walStorePath = walStorePath;

        return this;
    }

    /**
     * Gets write-ahead log archive path. Full WAL segments will be copied to this directory before reuse.
     *
     *  @return WAL archive directory.
     */
    public String getWalArchivePath() {
        return walArchivePath;
    }

    /**
     * Sets write-ahead log archive path. Full WAL segments will be copied to this directory before reuse.
     *
     * @param walArchivePath WAL archive directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalArchivePath(String walArchivePath) {
        this.walArchivePath = walArchivePath;

        return this;
    }
}
