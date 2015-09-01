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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for configuration of atomic data structures.
 */
public class VisorAtomicConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Atomic sequence reservation size. */
    private int seqReserveSize;

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Number of backups. */
    private int backups;

    /**
     * Create data transfer object for atomic configuration.
     *
     * @param src Atomic configuration.
     * @return Data transfer object.
     */
    public static VisorAtomicConfiguration from(AtomicConfiguration src) {
        VisorAtomicConfiguration cfg = new VisorAtomicConfiguration();

        cfg.seqReserveSize = src.getAtomicSequenceReserveSize();
        cfg.cacheMode = src.getCacheMode();
        cfg.backups = src.getBackups();

        return cfg;
    }

    /**
     * @return Atomic sequence reservation size.
     */
    public int atomicSequenceReserveSize() {
        return seqReserveSize;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /**
     * @return Number of backup nodes.
     */
    public int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorAtomicConfiguration.class, this);
    }
}