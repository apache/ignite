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
import org.apache.ignite.internal.processors.subscription.InternalSubscriber;

/**
 *
 */
public interface DatabaseLifecycleListener  extends InternalSubscriber {
    /**
     * Callback executed when data regions become to start-up.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    default void onInitDataRegions(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {};

    /**
     * Callback executed right before node become perform binary recovery.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    default void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {};

    /**
     * Callback executed when binary memory has fully restored and WAL logging is resumed.
     *
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    default void afterBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {};

    /**
     *
     * @param mgr
     * @throws IgniteCheckedException
     */
    default void beforeResumeWalLogging(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {};

    /**
     * @param mgr Database shared manager.
     */
    default void afterInitialise(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {};

    /**
     * @param mgr Database shared manager.
     */
    default void beforeStop(IgniteCacheDatabaseSharedManager mgr) {};
}
