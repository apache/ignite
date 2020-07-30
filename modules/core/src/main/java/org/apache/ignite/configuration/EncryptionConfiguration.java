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

/**
 * Encryption configuration.
 */
public class EncryptionConfiguration {
    /** Default number of threads used for re-encryption (equal to the number of available processors). */
    public static final int DFLT_REENCRYPTION_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    /** Default re-encryption rate limit. The value is {@code 0}, which means that scan speed is not limited. */
    public static final double DFLT_REENCRYPTION_RATE_MBPS = 0.0;

    /** Default number of pages that is scanned during reencryption under checkpoint lock. The value is {@code 100}. */
    public static final int DFLT_REENCRYPTION_BATCH_SIZE = 100;

    /** By default background re-encryption is enabled. */
    public static final boolean DFLT_REENCRYPTION_DISABLED = false;

    /** The number of threads used to scan partitions during re-encryption. */
    private int reencryptionThreadCnt = DFLT_REENCRYPTION_THREAD_POOL_SIZE;

    /** Re-encryption rate limit in megabytes per second (set {@code 0} for unlimited scanning). */
    private double reencryptionRateLimit = DFLT_REENCRYPTION_RATE_MBPS;

    /** The number of pages that is scanned during re-encryption under checkpoint lock. */
    private int reencryptionBatchSize = DFLT_REENCRYPTION_BATCH_SIZE;

    /** Set up this property to disable background re-encryption. */
    private boolean reencryptionDisabled = DFLT_REENCRYPTION_DISABLED;

    /**
     * Creates valid encryption configuration with all default values.
     */
    public EncryptionConfiguration() {
        // No-op.
    }

    /**
     * Constructs the copy of the configuration.
     *
     * @param cfg Configuration to copy.
     */
    public EncryptionConfiguration(EncryptionConfiguration cfg) {
        assert cfg != null;

        reencryptionBatchSize = cfg.getReencryptionBatchSize();
        reencryptionDisabled = cfg.isReencryptionDisabled();
        reencryptionRateLimit = cfg.getReencryptionRateLimit();
        reencryptionThreadCnt = cfg.getReencryptionThreadCnt();
    }

    /**
     * Gets the number of threads used for re-encryption.
     *
     * @return The number of threads used to scan partitions during re-encryption.
     */
    public int getReencryptionThreadCnt() {
        return reencryptionThreadCnt;
    }

    /**
     * Sets the number of threads used for re-encryption.
     *
     * @param reencryptionThreadCnt The number of threads used to scan partitions during re-encryption.
     * @return {@code this} for chaining.
     */
    public EncryptionConfiguration setReencryptionThreadCnt(int reencryptionThreadCnt) {
        this.reencryptionThreadCnt = reencryptionThreadCnt;

        return this;
    }

    /**
     * Gets re-encryption rate limit.
     *
     * @return Re-encryption rate limit in megabytes per second.
     */
    public double getReencryptionRateLimit() {
        return reencryptionRateLimit;
    }

    /**
     * Sets re-encryption rate limit.
     *
     * @param reencryptionRateLimit Re-encryption rate limit in megabytes per second.
     * @return {@code this} for chaining.
     */
    public EncryptionConfiguration setReencryptionRateLimit(double reencryptionRateLimit) {
        this.reencryptionRateLimit = reencryptionRateLimit;

        return this;
    }

    /**
     * Gets the disabled status of background re-encryption.
     *
     * @return {@code True} if background re-encryption is disabled.
     */
    public boolean isReencryptionDisabled() {
        return reencryptionDisabled;
    }

    /**
     * Sets the disabled status for background re-encryption.
     *
     * @param reencryptionDisabled Set {@code True} to disable background re-encryption.
     * @return {@code this} for chaining.
     */
    public EncryptionConfiguration setReencryptionDisabled(boolean reencryptionDisabled) {
        this.reencryptionDisabled = reencryptionDisabled;

        return this;
    }

    /**
     * Gets the number of pages that is scanned during re-encryption under checkpoint lock.
     *
     * @return The number of pages that is scanned during re-encryption under checkpoint lock.
     */
    public int getReencryptionBatchSize() {
        return reencryptionBatchSize;
    }

    /**
     * Sets the number of pages that is scanned during re-encryption under checkpoint lock.
     *
     * @param reencryptionBatchSize The number of pages that is scanned during re-encryption under checkpoint lock.
     * @return {@code this} for chaining.
     */
    public EncryptionConfiguration setReencryptionBatchSize(int reencryptionBatchSize) {
        this.reencryptionBatchSize = reencryptionBatchSize;

        return this;
    }
}
