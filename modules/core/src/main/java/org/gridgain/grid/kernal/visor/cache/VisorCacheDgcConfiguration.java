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

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for DGC configuration properties.
 */
public class VisorCacheDgcConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** DGC check frequency. */
    private long freq;

    /** DGC remove locks flag. */
    private boolean rmvLocks;

    /** Timeout for considering lock to be suspicious. */
    private long suspectLockTimeout;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for DGC configuration properties.
     */
    public static VisorCacheDgcConfiguration from(CacheConfiguration ccfg) {
        VisorCacheDgcConfiguration cfg = new VisorCacheDgcConfiguration();

        return cfg;
    }

    /**
     * @return DGC check frequency.
     */
    public long frequency() {
        return freq;
    }

    /**
     * @param freq New dGC check frequency.
     */
    public void frequency(long freq) {
        this.freq = freq;
    }

    /**
     * @return DGC remove locks flag.
     */
    public boolean removedLocks() {
        return rmvLocks;
    }

    /**
     * @param rmvLocks New dGC remove locks flag.
     */
    public void removedLocks(boolean rmvLocks) {
        this.rmvLocks = rmvLocks;
    }

    /**
     * @return Timeout for considering lock to be suspicious.
     */
    public long suspectLockTimeout() {
        return suspectLockTimeout;
    }

    /**
     * @param suspectLockTimeout New timeout for considering lock to be suspicious.
     */
    public void suspectLockTimeout(long suspectLockTimeout) {
        this.suspectLockTimeout = suspectLockTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheDgcConfiguration.class, this);
    }
}
