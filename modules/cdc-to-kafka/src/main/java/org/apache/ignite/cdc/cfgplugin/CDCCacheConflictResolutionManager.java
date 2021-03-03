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

package org.apache.ignite.cdc.cfgplugin;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.lang.IgniteFuture;

public class CDCCacheConflictResolutionManager implements CacheConflictResolutionManager  {
    @Override public CacheVersionConflictResolver conflictResolver() {
        return null;
    }

    @Override public void start(GridCacheContext cctx) throws IgniteCheckedException {

    }

    @Override public void stop(boolean cancel, boolean destroy) {

    }

    @Override public void onKernalStart() throws IgniteCheckedException {

    }

    @Override public void onKernalStop(boolean cancel) {

    }

    @Override public void printMemoryStats() {

    }

    @Override public void onDisconnected(IgniteFuture reconnectFut) {

    }
}
