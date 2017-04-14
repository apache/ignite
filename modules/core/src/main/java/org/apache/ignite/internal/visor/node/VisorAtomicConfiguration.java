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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for configuration of atomic data structures.
 */
public class VisorAtomicConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Atomic sequence reservation size. */
    private int seqReserveSize;

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Number of backups. */
    private int backups;

    /**
     * Default constructor.
     */
    public VisorAtomicConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for atomic configuration.
     *
     * @param src Atomic configuration.
     */
    public VisorAtomicConfiguration(AtomicConfiguration src) {
        seqReserveSize = src.getAtomicSequenceReserveSize();
        cacheMode = src.getCacheMode();
        backups = src.getBackups();
    }

    /**
     * @return Atomic sequence reservation size.
     */
    public int getAtomicSequenceReserveSize() {
        return seqReserveSize;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * @return Number of backup nodes.
     */
    public int getBackups() {
        return backups;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(seqReserveSize);
        U.writeEnum(out, cacheMode);
        out.writeInt(backups);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        seqReserveSize = in.readInt();
        cacheMode = CacheMode.fromOrdinal(in.readByte());
        backups = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorAtomicConfiguration.class, this);
    }
}
