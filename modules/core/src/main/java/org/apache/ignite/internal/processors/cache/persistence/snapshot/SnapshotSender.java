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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.marshaller.MappedName;

/**
 *
 */
abstract class SnapshotSender implements Closeable {
    /** Busy processing lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** {@code true} if sender is currently working */
    private volatile boolean stopped;

    /** Ignite logger to use. */
    protected final IgniteLogger log;

    /**
     * @param log Ignite logger to use.
     */
    protected SnapshotSender(IgniteLogger log) {
        this.log = log.getLogger(SnapshotSender.class);
    }

    /**
     * @param mappings Local node marshaller mappings.
     */
    public final void sendMarshallerMeta(List<Map<Integer, MappedName>> mappings) {
        if (!lock.readLock().tryLock())
            return;

        try {
            if (stopped)
                return;

            if (mappings == null)
                return;

            sendMarshallerMeta0(mappings);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param types Collection of known binary types.
     */
    public final void sendBinaryMeta(Map<Integer, BinaryType> types) {
        if (!lock.readLock().tryLock())
            return;

        try {
            if (stopped)
                return;

            if (types == null)
                return;

            sendBinaryMeta0(types);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param ccfg Cache configuration file.
     * @param cacheDirName Cache group directory name.
     */
    public final void sendCacheConfig(File ccfg, String cacheDirName) {
        if (!lock.readLock().tryLock())
            return;

        try {
            if (stopped)
                return;

            sendCacheConfig0(ccfg, cacheDirName);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param part Partition file to send.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     * @param length Partition length.
     */
    public final void sendPart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
        if (!lock.readLock().tryLock())
            return;

        try {
            if (stopped)
                return;

            sendPart0(part, cacheDirName, pair, length);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param delta Delta pages file.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     */
    public final void sendDelta(File delta, String cacheDirName, GroupPartitionId pair) {
        if (!lock.readLock().tryLock())
            return;

        try {
            if (stopped)
                return;

            sendDelta0(delta, cacheDirName, pair);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IOException {
        lock.writeLock().lock();

        try {
            stopped = true;

            close0();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param part Partition file to send.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     * @param len Partition length.
     */
    protected abstract void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len);

    /**
     * @param delta Delta pages file.
     * @param cacheDirName Cache group directory name.
     * @param pair Group id with partition id pair.
     */
    protected abstract void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair);

    /**
     * @param mappings Local node marshaller mappings.
     */
    protected void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
        // No-op by default.
    }

    /**
     * @param types Collection of known binary types.
     */
    protected void sendBinaryMeta0(Map<Integer, BinaryType> types) {
        // No-op by default.
    }

    /**
     * @param ccfg Cache configuration file.
     * @param cacheDirName Cache group directory name.
     */
    protected void sendCacheConfig0(File ccfg, String cacheDirName) {
        // No-op by default.
    }

    /**
     * @throws IOException If fails.
     */
    protected void close0() throws IOException {
        // No-op by default.
    }
}
