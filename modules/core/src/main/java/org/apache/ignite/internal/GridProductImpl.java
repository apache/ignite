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

package org.apache.ignite.internal;

import org.apache.ignite.internal.product.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * {@link GridProduct} implementation.
 */
public class GridProductImpl implements GridProduct {
    /** */
    private GridKernalContext ctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridProductImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     */
    public GridProductImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public long gracePeriodLeft() {
        ctx.gateway().readLock();

        try {
            return ctx.license().gracePeriodLeft();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteProductLicense license() {
        ctx.gateway().readLock();

        try {
            return ctx.license().license();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void updateLicense(String lic) throws IgniteProductLicenseException {
        ctx.gateway().readLock();

        try {
            ctx.license().updateLicense(lic);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }
}
