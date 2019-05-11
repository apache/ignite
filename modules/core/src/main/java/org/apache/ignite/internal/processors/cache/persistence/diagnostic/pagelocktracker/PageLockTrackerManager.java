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

import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

/**
 * Page lock manager.
 */
public class PageLockTrackerManager {
    /** MXbean */
    private final PageLockTrackerMXBean mxBean;

    /** */
    private final SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker();

    /**
     * Default constructor.
     */
    public PageLockTrackerManager() {
        mxBean = new PageLockTrackerMXBeanImpl(this);
    }

    /**
     * @param name Lock tracker name.
     * @return Instance of {@link PageLockListener} for tracking lock/unlock operations.
     */
    public PageLockListener createPageLockTracker(String name) {
        return sharedPageLockTracker.registrateStructure(name);
    }

    /**
     * Enable page lock tracking.
     */
    public void enableTracking() {

    }

    /**
     * Disable page lock tracking.
     */
    public void disableTracking() {

    }

    /**
     * Check page lock tracking.
     *
     * @return {@code True} if tracking enable, {@code False} if disable.
     */
    public boolean isTracingEnable() {
        return false;
    }

    /**
     * Take page locks dump.
     *
     * @return String representation of page locks dump.
     */
    public String dumpLocks() {
        return null;
    }

    /**
     * Take page locks dump and print it to console.
     */
    public void dumpLocksToLog() {

    }

    /**
     * Take page locks dump and save to file.
     *
     * @return Absolute file path.
     */
    public String dumpLocksToFile() {
        return null;
    }

    /**
     * Take page locks dump and save to file for specific path.
     *
     * @param path Path to save file.
     * @return Absolute file path.
     */
    public String dumpLocksToFile(String path) {
        return null;
    }

    /**
     * Getter.
     *
     * @return PageLockTrackerMXBean object.
     */
    public PageLockTrackerMXBean mxBean() {
        return mxBean;
    }
}
