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

package org.apache.ignite.internal.processors.interop.os;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.interop.*;
import org.jetbrains.annotations.*;

/**
 * OS interop processor.
 */
public class GridOsInteropProcessor extends GridInteropProcessorAdapter {
    /** Common error message. */
    private static final String ERR_MSG = "Interop feature is not supported in OS edition.";

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public GridOsInteropProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void releaseStart() {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public void awaitStart() throws IgniteCheckedException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public long environmentPointer() throws IgniteCheckedException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public String gridName() {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridInteropTarget projection() throws IgniteCheckedException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridInteropTarget cache(@Nullable String name) throws IgniteCheckedException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridInteropTarget dataLoader(@Nullable String cacheName) throws IgniteCheckedException {
        throw new UnsupportedOperationException(ERR_MSG);
    }
}
