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
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTracker.State;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Page lock manager.
 */
public class PageLockTrackerManager {
    /** MXbean */
    private final PageLockTrackerMXBean mxBean;

    /** */
    private final SharedPageLockTracker sharedPageLockTracker;

    /** */
    private final IgniteLogger log;

    /** */
    private Set<State> threads;

    /**
     * Default constructor.
     */
    public PageLockTrackerManager(IgniteLogger log) {
        this.mxBean = new PageLockTrackerMXBeanImpl(this);
        this.sharedPageLockTracker = new SharedPageLockTracker(this::onHangThreads);
        this.log = log;

        sharedPageLockTracker.onStart();
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
                ToFileDumpProcessor.toFileDump(dump, new File(U.defaultWorkDirectory()));
            }
            catch (IgniteCheckedException e) {
                log.warning("Faile to save locks dump file.", e);
            }
        }
    }

    /**
     * @param name Lock tracker name.
     * @return Instance of {@link PageLockListener} for tracking lock/unlock operations.
     */
    public PageLockListener createPageLockTracker(String name) {
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
            return ToFileDumpProcessor.toFileDump(dump, new File(U.defaultWorkDirectory()));
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
            return ToFileDumpProcessor.toFileDump(dump, new File(path));
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
        return 0L;
    }

    /**
     * @return Total offheap overhead in bytes.
     */
    public long getOffHeapOverhead() {
        return 0L;
    }

    /**
     * @return Total overhead in bytes.
     */
    public long getTotalOverhead() {
        return getHeapOverhead() + getOffHeapOverhead();
    }
}
