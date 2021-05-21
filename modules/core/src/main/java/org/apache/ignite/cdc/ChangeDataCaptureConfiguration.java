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

package org.apache.ignite.cdc;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * This class defines {@link ChangeDataCapture} runtime configuration.
 * This configuration is passed to {@link ChangeDataCapture} constructor.
 * It defines all configuration
 */
@IgniteExperimental
public class ChangeDataCaptureConfiguration {
    /** */
    private static final int DFLT_LOCK_TIMEOUT = 1000;

    /** */
    private static final long DFLT_CHECK_FREQ = 1000L;

    /** */
    private static final boolean DFLT_KEEP_BINARY = true;

    /** Change Data Capture consumer. */
    private ChangeDataCaptureConsumer consumer;

    /** Keep binary flag.<br>Default value {@code true}. */
    private boolean keepBinary = DFLT_KEEP_BINARY;

    /**
     * {@link ChangeDataCapture} acquire file lock on startup to ensure exclusive consumption.
     * This property specifies amount of time to wait for lock acquisition.<br>
     * Default is {@code 1000 ms}.
     */
    private long lockTimeout = DFLT_LOCK_TIMEOUT;

    /**
     * CDC application periodically scans {@link DataStorageConfiguration#getCdcPath()} folder to find new WAL segments.
     * This frequency specify amount of time application sleeps between subsequent checks when no new files available.
     * Default is {@code 1000 ms}.
     */
    private long checkFrequency = DFLT_CHECK_FREQ;

    /** @return CDC consumer. */
    public ChangeDataCaptureConsumer getConsumer() {
        return consumer;
    }

    /** @param consumer CDC consumer. */
    public void setConsumer(ChangeDataCaptureConsumer consumer) {
        this.consumer = consumer;
    }

    /** @return keep binary value. */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /** @param keepBinary keep binary value. */
    public void setKeepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /** @return Amount of time to wait for lock acquisition. */
    public long getLockTimeout() {
        return lockTimeout;
    }

    /** @param lockTimeout Amount of time to wait for lock acquisition. */
    public void setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    /** @return Amount of time application sleeps between subsequent checks when no new files available. */
    public long getCheckFrequency() {
        return checkFrequency;
    }

    /**
     * @param checkFrequency Amount of time application sleeps between subsequent checks when no new
     *                                           files available.
     */
    public void setCheckFrequency(long checkFrequency) {
        this.checkFrequency = checkFrequency;
    }
}
