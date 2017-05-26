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

    /** WAL persistence path. */
    private String walStorePath;

    /** WAL archive path. */
    private String walArchivePath;

    /**
     * Returns a path the root directory where the Persistent Store will persist data and indexes.
     */
    public String getPersistentStorePath() {
        return persistenteStorePath;
    }

    /**
     * Sets a path to the root directory where the Persistent Store will persist data and indexes.
     * By default the Persistent Store's files are located under Ignite work directory.
     *
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
     * Sets the checkpoint frequency which is a minimal interval when the memory state (updated data, indexes, etc.)
     * will be written to the Persistent Store. If the rate is high, checkpoints can happen more frequently.
     *
     * @param checkpointFreq Checkpoint frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointFrequency(long checkpointFreq) {
        this.checkpointFreq = checkpointFreq;

        return this;
    }

    /**
     * Gets amount of memory allocated for a checkpoint temporary buffer.
     *
     * @return Checkpoint page buffer size.
     */
    public Long getCheckpointPageBufferSize() {
        return checkpointPageBufSize;
    }

    /**
     * Sets amount of memory allocated for the checkpoint temporary buffer. The buffer is used to create temporary
     * copies of pages when the checkpoint process is in progress.
     *
     * @param checkpointPageBufSize Checkpoint page buffer size.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointPageBufferSize(long checkpointPageBufSize) {
        this.checkpointPageBufSize = checkpointPageBufSize;

        return this;
    }


    /**
     * Gets a number of threads to use for the checkpoint purposes.
     *
     * @return Number of checkpoint threads.
     */
    public int getCheckpointThreads() {
        return checkpointThreads;
    }

    /**
     * Sets a number of threads to use for the checkpoint purposes
     *
     * @param checkpointThreads Number of checkpoint threads. One thread is used by default.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointThreads(int checkpointThreads) {
        this.checkpointThreads = checkpointThreads;

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
     * Gets a total number of checkpoints to keep in the WAL history.
     *
     * @return Number of WAL segments to keep after a checkpoint is finished.
     */
    public int getWalHistorySize() {
        return walHistSize <= 0 ? DFLT_WAL_HISTORY_SIZE : walHistSize;
    }

    /**
     * Sets a total number of checkpoints to keep in the WAL history.
     *
     * @param walHistSize Number of WAL segments to keep after a checkpoint is finished.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalHistorySize(int walHistSize) {
        this.walHistSize = walHistSize;

        return this;
    }

    /**
     * Gets a number of WAL segments to work with.
     *
     * @return Number of work WAL segments.
     */
    public int getWalSegments() {
        return walSegments <= 0 ? DFLT_WAL_SEGMENTS : walSegments;
    }

    /**
     * Sets a number of WAL segments to work with. For performance reasons,
     * the whole WAL is split into files of fixed length called segments.
     *
     * @param walSegments Number of WAL segments.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegments(int walSegments) {
        this.walSegments = walSegments;

        return this;
    }

    /**
     * Gets size of a WAL segment.
     *
     * @return WAL segment size.
     */
    public int getWalSegmentSize() {
        return walSegmentSize <= 0 ? DFLT_WAL_SEGMENT_SIZE : walSegmentSize;
    }

    /**
     * Sets size of a WAL segment.
     *
     * @param walSegmentSize WAL segment size. 64 MB is used by default.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegmentSize(int walSegmentSize) {
        this.walSegmentSize = walSegmentSize;

        return this;
    }

    /**
     * Gets a path to the directory where WAL is stored.
     *
     * @return WAL persistence path, absolute or relative to Ignite work directory.
     */
    public String getWalStorePath() {
        return walStorePath;
    }

    /**
     * Sets a path to the directory where WAL is stored . If this path is relative, it will be resolved
     * relatively to Ignite work directory.
     *
     * @param walStorePath WAL persistence path, absolute or relative to Ignite work directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalStorePath(String walStorePath) {
        this.walStorePath = walStorePath;

        return this;
    }

    /**
     * Gets a path to the WAL archive directory.
     *
     *  @return WAL archive directory.
     */
    public String getWalArchivePath() {
        return walArchivePath;
    }

    /**
     * Sets a path for the WAL archive directory. Every WAL segment will be fully copied to this directory before
     * it can be reused for WAL purposes.
     *
     * @param walArchivePath WAL archive directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalArchivePath(String walArchivePath) {
        this.walArchivePath = walArchivePath;

        return this;
    }
}
