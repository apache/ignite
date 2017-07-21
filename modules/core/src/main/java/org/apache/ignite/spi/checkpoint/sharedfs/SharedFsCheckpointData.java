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

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper of all checkpoint that are saved to the file system. It
 * extends every checkpoint with expiration time and host name
 * which created this checkpoint.
 * <p>
 * Host name is used by {@link SharedFsCheckpointSpi} SPI to give node
 * correct files if it is restarted.
 */
class SharedFsCheckpointData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Checkpoint data. */
    private final byte[] state;

    /** Checkpoint expiration time. */
    private final long expTime;

    /** Host name that created this checkpoint. */
    private final String host;

    /** Checkpoint key. */
    private final String key;

    /**
     * Creates new instance of checkpoint data wrapper.
     *
     * @param state Checkpoint data.
     * @param expTime Checkpoint expiration time in milliseconds.
     * @param host Name of host that created this checkpoint.
     * @param key Key of checkpoint.
     */
    SharedFsCheckpointData(byte[] state, long expTime, String host, String key) {
        assert expTime >= 0;
        assert host != null;

        this.state = state;
        this.expTime = expTime;
        this.host = host;
        this.key = key;
    }

    /**
     * Gets checkpoint data.
     *
     * @return Checkpoint data.
     */
    byte[] getState() {
        return state;
    }

    /**
     * Gets checkpoint expiration time.
     *
     * @return Expire time in milliseconds.
     */
    long getExpireTime() {
        return expTime;
    }

    /**
     * Gets checkpoint host name.
     *
     * @return Host name.
     */
    String getHost() {
        return host;
    }

    /**
     * Gets key of checkpoint.
     *
     * @return Key of checkpoint.
     */
    public String getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SharedFsCheckpointData.class, this);
    }
}