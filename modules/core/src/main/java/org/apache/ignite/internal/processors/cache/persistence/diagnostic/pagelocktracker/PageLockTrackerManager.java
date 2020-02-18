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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.io.File;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTracker.State;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.NotNull;

import static java.io.File.separatorChar;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_TYPE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;

/**
 * Page lock manager.
 */
public class PageLockTrackerManager implements LifecycleAware {
    /** */
    private static final long OVERHEAD_SIZE = 16 + 8  + 8 + 8 + 8;

    /** */
    private final MemoryCalculator memoryCalculator = new MemoryCalculator();

    /** MXbean */
    private final PageLockTrackerMXBean mxBean;

    /** */
    private final SharedPageLockTracker sharedPageLockTracker;

    /** */
    private final IgniteLogger log;

    /** */
    private Set<State> threads;

    /** */
    private final String managerNameId;

    /** */
    private final boolean trackingEnable;

    /**
     * Default constructor.
     */
    public PageLockTrackerManager(IgniteLogger log) {
        this(log, "mgr_" + UUID.randomUUID().toString());
    }

    /**
     * @param log Ignite logger.
     * @param managerNameId Manager name.
     */
    public PageLockTrackerManager(IgniteLogger log, String managerNameId) {
        this.trackingEnable = !(getInteger(IGNITE_PAGE_LOCK_TRACKER_TYPE, HEAP_LOG) == -1);
        this.managerNameId = managerNameId;
        this.mxBean = new PageLockTrackerMXBeanImpl(this, memoryCalculator);
        this.sharedPageLockTracker = new SharedPageLockTracker(this::onHangThreads, memoryCalculator);
        this.log = log;

        memoryCalculator.onHeapAllocated(OVERHEAD_SIZE);
    }

    /**
     * @param threads Hang threads.
     */
    private void onHangThreads(@NotNull Set<State> threads) {
        assert threads != null;

        // Processe only one for same list thread state.
        // Protection of spam.
        if (!threads.equals(this.threads)) {
            this.threads = threads;

            ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

            StringBuilder sb = new StringBuilder();

            threads.forEach(s -> {
                Thread th = s.thread;
                sb.append("(")
                    .append(th.getName())
                    .append("-")
                    .append(th.getId())
                    .append(", ")
                    .append(th.getState())
                    .append(")");

            });

            log.warning("Threads hanged: [" + sb + "]");
            // If some thread is hangs
            // Print to log.
            log.warning(ToStringDumpProcessor.toStringDump(dump));

            try {
                // Write dump to file.
                ToFileDumpProcessor.toFileDump(dump, new File(U.defaultWorkDirectory() +
                    separatorChar + DEFAULT_TARGET_FOLDER +  separatorChar), managerNameId);
            }
            catch (IgniteCheckedException e) {
                log.warning("Failed to save locks dump file.", e);
            }
        }
    }

    /**
     * @param name Lock tracker name.
     * @return Instance of {@link PageLockListener} for tracking lock/unlock operations.
     */
    public PageLockListener createPageLockTracker(String name) {
        if (!trackingEnable)
            return DataStructure.NOOP_LSNR;

        return sharedPageLockTracker.registrateStructure(name);
    }

    /**
     * Take page locks dump.
     *
     * @return String representation of page locks dump.
     */
    public String dumpLocks() {
        ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

        return ToStringDumpProcessor.toStringDump(dump);
    }

    /**
     * Take page locks dump and print it to console.
     */
    public void dumpLocksToLog() {
        log.warning(dumpLocks());
    }

    /**
     * Take page locks dump and save to file.
     *
     * @return Absolute file path.
     */
    public String dumpLocksToFile() {
        ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

        try {
            return ToFileDumpProcessor.toFileDump(dump,
                new File(U.defaultWorkDirectory() +
                    File.separatorChar + DEFAULT_TARGET_FOLDER +  File.separatorChar), managerNameId);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Take page locks dump and save to file for specific path.
     *
     * @param path Path to save file.
     * @return Absolute file path.
     */
    public String dumpLocksToFile(String path) {
        ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

        try {
            return ToFileDumpProcessor.toFileDump(dump, new File(path), managerNameId);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Getter.
     *
     * @return PageLockTrackerMXBean object.
     */
    public PageLockTrackerMXBean mxBean() {
        return mxBean;
    }

    /**
     * @return Total heap overhead in bytes.
     */
    public long getHeapOverhead() {
        return memoryCalculator.heapUsed.get();
    }

    /**
     * @return Total offheap overhead in bytes.
     */
    public long getOffHeapOverhead() {
        return memoryCalculator.offHeapUsed.get();
    }

    /**
     * @return Total overhead in bytes.
     */
    public long getTotalOverhead() {
        return getHeapOverhead() + getOffHeapOverhead();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        sharedPageLockTracker.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        sharedPageLockTracker.stop();
    }

    /**
     *
     */
   public static class MemoryCalculator {
        /** */
        private final AtomicLong heapUsed = new AtomicLong();

        /** */
        private final AtomicLong offHeapUsed = new AtomicLong();

        /** */
        MemoryCalculator(){
            onHeapAllocated(16 + (8 + 16) * 2);
        }

        /** */
        public void onHeapAllocated(long bytes) {
            assert bytes >= 0;

            heapUsed.getAndAdd(bytes);
        }

        /** */
        public void onOffHeapAllocated(long bytes) {
            assert bytes >= 0;

            offHeapUsed.getAndAdd(bytes);
        }

        /** */
        public void onHeapFree(long bytes) {
            heapUsed.getAndAdd(-bytes);
        }

        /** */
        public void onOffHeapFree(long bytes) {
            offHeapUsed.getAndAdd(-bytes);
        }
    }
}
