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

package org.apache.ignite.spi.indexing;

import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Default implementation of {@link GridIndexingSpi} which does not index cache.
 */
@IgniteSpiNoop
public class GridNoopIndexingSpi extends IgniteSpiAdapter implements GridIndexingSpi {
    /** {@inheritDoc} */
    @Override public Iterator<?> query(@Nullable String spaceName, Collection<Object> params,
        @Nullable GridIndexingQueryFilter filters) throws IgniteSpiException {
        throw new IgniteSpiException("You have to configure custom GridIndexingSpi implementation.");
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String spaceName, Object key, Object val, long expirationTime)
        throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, Object key) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }
}
