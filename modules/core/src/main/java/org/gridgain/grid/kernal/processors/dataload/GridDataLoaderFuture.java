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

package org.gridgain.grid.kernal.processors.dataload;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Data loader future.
 */
class GridDataLoaderFuture extends GridFutureAdapter<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data loader. */
    @GridToStringExclude
    private IgniteDataLoader dataLdr;

    /**
     * Default constructor for {@link Externalizable} support.
     */
    public GridDataLoaderFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param dataLdr Data loader.
     */
    GridDataLoaderFuture(GridKernalContext ctx, IgniteDataLoader dataLdr) {
        super(ctx);

        assert dataLdr != null;

        this.dataLdr = dataLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        checkValid();

        if (onCancelled()) {
            dataLdr.close(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoaderFuture.class, this, super.toString());
    }
}
