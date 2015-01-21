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

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.managed.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Service deployment future.
 */
public class GridServiceDeploymentFuture extends GridFutureAdapter<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ManagedServiceConfiguration cfg;

    /**
     * @param ctx Context.
     * @param cfg Configuration.
     */
    public GridServiceDeploymentFuture(GridKernalContext ctx, ManagedServiceConfiguration cfg) {
        super(ctx);

        this.cfg = cfg;
    }

    /**
     * @return Service configuration.
     */
    ManagedServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeploymentFuture.class, this);
    }
}
