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

package org.apache.ignite.internal.visor;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Base class for Visor jobs.
 */
public abstract class VisorJob<A, R> extends ComputeJobAdapter {
    @IgniteInstanceResource
    protected transient GridEx g;

    /** Job start time. */
    protected transient long start;

    /** Debug flag. */
    protected transient boolean debug;

    /**
     * Create job with specified argument.
     *
     * @param arg Job argument.
     */
    protected VisorJob(@Nullable A arg, boolean debug) {
        super(arg);

        this.debug = debug;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute() throws IgniteCheckedException {
        start = U.currentTimeMillis();

        A arg = argument(0);

        try {
            if (debug)
                logStart(g.log(), getClass(), start);

            return run(arg);
        }
        finally {
            if (debug)
                logFinish(g.log(), getClass(), start);
        }
    }

    /**
     * Execution logic of concrete task.
     *
     * @return Result.
     */
    protected abstract R run(@Nullable A arg) throws IgniteCheckedException;
}
