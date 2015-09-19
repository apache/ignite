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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.jetbrains.annotations.Nullable;

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
        return null;
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
    @Override public PlatformTarget cache(@Nullable String name) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget createCache(@Nullable String name) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget getOrCreateCache(@Nullable String name) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget affinity(@Nullable String name) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget dataStreamer(@Nullable String cacheName, boolean keepPortable) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget projection() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget compute(PlatformTarget grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget message(PlatformTarget grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget events(PlatformTarget grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget services(PlatformTarget grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget extensions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerStore(PlatformCacheStore store, boolean convertPortable)
        throws IgniteCheckedException {
        // No-op.
    }
}
