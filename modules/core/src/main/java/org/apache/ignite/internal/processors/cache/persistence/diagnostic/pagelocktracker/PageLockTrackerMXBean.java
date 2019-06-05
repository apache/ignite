/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * This interface defines JMX managment interface for page lock tracking.
 */
@MXBeanDescription("MBean that provides access to page lock tracking.")
public interface PageLockTrackerMXBean {
    /** */
    public static final String MBEAN_NAME = "PageLockTracker";
    /**
     * Take page locks dump.
     *
     * @return String representation of page locks dump.
     */
    @MXBeanDescription("Take page locks dump.")
    String dumpLocks();

    /**
     * Take page locks dump and print it to console.
     */
    @MXBeanDescription("Take page locks dump and print it to console.")
    void dumpLocksToLog();

    /**
     * Take page locks dump and save to file.
     *
     * @return Absolute file path.
     */
    @MXBeanDescription("Take page locks dump and save to file.")
    String dumpLocksToFile();

    /**
     * Take page locks dump and save to file for specific path.
     *
     * @param path Path to save file.
     * @return Absolute file path.
     */
    @MXBeanDescription("Take page locks dump and save to file for specific path.")
    String dumpLocksToFile(String path);
}