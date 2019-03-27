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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.IgniteInternalFuture;

/**
 *
 */
public interface CacheDataStoreProxy extends IgniteCacheOffheapManager.CacheDataStore {
    /**
     * @param mode The mode to associate with data storage instance.
     * @param storage The cache data storage instance to set to.
     * @return <tt>true</tt> if the storage have been changed successfully.
     */
    public boolean storage(StorageMode mode, IgniteCacheOffheapManager.CacheDataStore storage);

    /**
     * @param mode The mode to switch to.
     */
    public void storageMode(StorageMode mode);

    /**
     * @return The currently used storage mode. Some of the long-running threads will remain to use
     * the old mode until they finish.
     */
    public StorageMode storageMode();

    /**
     *
     */
    public enum StorageMode {
        /** Proxy will normally route all operations to the PageMemrory. */
        FULL,

        /** Proxy will redirect the write operations to the temp-WAL storage. */
        LOG_ONLY;
    }
}
