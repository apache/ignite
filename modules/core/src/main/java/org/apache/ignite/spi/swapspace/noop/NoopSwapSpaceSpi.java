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

package org.apache.ignite.spi.swapspace.noop;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation of {@link org.apache.ignite.spi.swapspace.SwapSpaceSpi}. Exists for testing and benchmarking purposes.
 */
@IgniteSpiNoop
@IgniteSpiMultipleInstancesSupport(true)
public class NoopSwapSpaceSpi extends IgniteSpiAdapter implements SwapSpaceSpi {
    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        U.warn(log, "Swap space is disabled. To enable use GridFileSwapSpaceSpi.");
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void clear(@Nullable String space) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String space) throws IgniteSpiException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long count(@Nullable String space) throws IgniteSpiException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override @Nullable public byte[] read(@Nullable String spaceName, SwapKey key, SwapContext ctx)
        throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<SwapKey, byte[]> readAll(@Nullable String spaceName, Iterable<SwapKey> keys,
        SwapContext ctx) throws IgniteSpiException {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, SwapKey key, @Nullable IgniteInClosure<byte[]> c,
        SwapContext ctx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable String spaceName, Collection<SwapKey> keys,
        @Nullable IgniteBiInClosure<SwapKey, byte[]> c, SwapContext ctx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, SwapKey key, @Nullable byte[] val,
        SwapContext ctx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void storeAll(@Nullable String spaceName, Map<SwapKey, byte[]> pairs,
        SwapContext ctx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable SwapSpaceSpiListener evictLsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> partitions(@Nullable String spaceName) throws IgniteSpiException {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <K> IgniteSpiCloseableIterator<K> keyIterator(@Nullable String spaceName,
        SwapContext ctx) throws IgniteSpiException {
        return new GridEmptyCloseableIterator<>();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(
        @Nullable String spaceName) throws IgniteSpiException {
        return new GridEmptyCloseableIterator<>();
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName,
        int part) throws IgniteSpiException {
        return new GridEmptyCloseableIterator<>();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NoopSwapSpaceSpi.class, this);
    }
}
