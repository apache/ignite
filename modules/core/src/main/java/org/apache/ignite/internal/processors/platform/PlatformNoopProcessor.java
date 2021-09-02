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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheManager;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;

/**
 * No-op processor.
 */
public class PlatformNoopProcessor extends GridProcessorAdapter implements PlatformProcessor {
    public PlatformNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long environmentPointer() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public PlatformContext context() {
        throw new IgniteException("Platforms are not available [nodeId=" + ctx.grid().localNode().id() + "] " +
            "(Use Apache.Ignite.Core.Ignition.Start() or Apache.Ignite.exe to start Ignite.NET nodes; " +
            "ignite::Ignition::Start() or ignite.exe to start Ignite C++ nodes).");
    }

    /** {@inheritDoc} */
    @Override public boolean hasContext() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void releaseStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void awaitStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void registerStore(PlatformCacheStore store, boolean convertBinary)
        throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public PlatformCacheManager cacheManager() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void setThreadLocal(Object value) {
        // No-op.
    }
}
