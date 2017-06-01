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
    public static final int DFLT_CHECKPOINTING_FREQ = 180000;

    /** Lock default wait time, 10 sec. */
    public static final int DFLT_LOCK_WAIT_TIME = 10 * 1000;

    /** */
    @SuppressWarnings("UnnecessaryBoxing")
    public static final Long DFLT_CHECKPOINTING_PAGE_BUFFER_SIZE = new Long(256L * 1024 * 1024);

    /** Default number of checkpointing threads. */
    public static final int DFLT_CHECKPOINTING_THREADS = 1;

    /** */
    private static final int DFLT_WAL_HISTORY_SIZE = 20;

    /** */
    public static final int DFLT_WAL_SEGMENTS = 10;

    /** */
    private static final int DFLT_WAL_SEGMENT_SIZE = 64 * 1024 * 1024;

    /** */
    private String persistenteStorePath;

    /** Checkpointing frequency. */
    private long checkpointingFreq = DFLT_CHECKPOINTING_FREQ;

    /** Lock wait time. */
    private int lockWaitTime = DFLT_LOCK_WAIT_TIME;

    /** */
    private Long checkpointingPageBufSize = DFLT_CHECKPOINTING_PAGE_BUFFER_SIZE;

    /** */
    private int checkpointingThreads = DFLT_CHECKPOINTING_THREADS;

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
     * Gets checkpointing frequency.
     *
     * @return checkpointing frequency in milliseconds.
     */
    public long getCheckpointingFrequency() {
        return checkpointingFreq <= 0 ? DFLT_CHECKPOINTING_FREQ : checkpointingFreq;
    }

    /**
     * Sets the checkpointing frequency which is a minimal interval when the dirty pages will be written
     * to the Persistent Store. If the rate is high, checkpointing will be triggered more frequently.
     *
     * @param checkpointingFreq checkpointing frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingFrequency(long checkpointingFreq) {
        this.checkpointingFreq = checkpointingFreq;

        return this;
    }

    /**
     * Gets amount of memory allocated for a checkpointing temporary buffer.
     *
     * @return checkpointing page buffer size in bytes.
     */
    public Long getCheckpointingPageBufferSize() {
        return checkpointingPageBufSize;
    }

    /**
     * Sets amount of memory allocated for the checkpointing temporary buffer. The buffer is used to create temporary
     * copies of pages that are being written to disk and being update in parallel while the checkpointing is in
     * progress.
     *
     * @param checkpointingPageBufSize checkpointing page buffer size in bytes.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingPageBufferSize(long checkpointingPageBufSize) {
        this.checkpointingPageBufSize = checkpointingPageBufSize;

        return this;
    }


    /**
     * Gets a number of threads to use for the checkpointing purposes.
     *
     * @return Number of checkpointing threads.
     */
    public int getCheckpointingThreads() {
        return checkpointingThreads;
    }

    /**
     * Sets a number of threads to use for the checkpointing purposes.
     *
     * @param checkpointingThreads Number of checkpointing threads. One thread is used by default.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingThreads(int checkpointingThreads) {
        this.checkpointingThreads = checkpointingThreads;

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
