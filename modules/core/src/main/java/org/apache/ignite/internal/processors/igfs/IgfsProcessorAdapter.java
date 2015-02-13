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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.igfs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.ipc.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ignite file system processor adapter.
 */
public abstract class IgfsProcessorAdapter extends GridProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected IgfsProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Gets all IGFS instances.
     *
     * @return Collection of IGFS instances.
     */
    public abstract Collection<IgniteFs> igfss();

    /**
     * Gets IGFS instance.
     *
     * @param name (Nullable) IGFS name.
     * @return IGFS instance.
     */
    @Nullable public abstract IgniteFs igfs(@Nullable String name);

    /**
     * Gets server endpoints for particular IGFS.
     *
     * @param name IGFS name.
     * @return Collection of endpoints or {@code null} in case IGFS is not defined.
     */
    public abstract Collection<IpcServerEndpoint> endpoints(@Nullable String name);

    /**
     * Create compute job for the given IGFS job.
     *
     * @param job IGFS job.
     * @param igfsName IGFS name.
     * @param path Path.
     * @param start Start position.
     * @param length Length.
     * @param recRslv Record resolver.
     * @return Compute job.
     */
    @Nullable public abstract ComputeJob createJob(IgfsJob job, @Nullable String igfsName, IgfsPath path,
        long start, long length, IgfsRecordResolver recRslv);
}
