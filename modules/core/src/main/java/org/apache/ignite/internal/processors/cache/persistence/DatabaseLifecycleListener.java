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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
@SuppressWarnings("RedundantThrows")
public interface DatabaseLifecycleListener {
    /**
     * Callback executed when data regions become to start-up.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    public default void onInitDataRegions(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {}

    /**
     * Callback executed when node detected that baseline topology is changed and node is not in that baseline.
     * It's useful to cleanup and invalidate all available data restored at that moment.
     *
     * @throws IgniteCheckedException If failed.
     */
    public default void onBaselineChange() throws IgniteCheckedException {}

    /**
     * Callback executed right before node become perform binary recovery.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    public default void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {}

    /**
     * Callback executed when binary memory has fully restored and WAL logging is resumed.
     *
     *
     * @param mgr
     * @param restoreState Result of binary recovery.
     * @throws IgniteCheckedException If failed.
     */
    public default void afterBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr,
        GridCacheDatabaseSharedManager.RestoreBinaryState restoreState) throws IgniteCheckedException {}

    /**
     * Callback executed when all logical updates were applied and page memory become to fully consistent state.
     *
     *
     * @param mgr
     * @param restoreState Result of logical recovery.
     * @throws IgniteCheckedException If failed.
     */
    public default void afterLogicalUpdatesApplied(IgniteCacheDatabaseSharedManager mgr,
        GridCacheDatabaseSharedManager.RestoreLogicalState restoreState) throws IgniteCheckedException {}

    /**
     * Callback executed when all physical updates are applied and we are ready to write new physical records
     * during logical recovery.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    public default void beforeResumeWalLogging(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {}

    /**
     * Callback executed after all data regions are initialized.
     *
     * @param mgr Database shared manager.
     */
    public default void afterInitialise(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {}

    /**
     * Callback executed before shared manager will be stopped.
     *
     * @param mgr Database shared manager.
     */
    public default void beforeStop(IgniteCacheDatabaseSharedManager mgr) {}
}
