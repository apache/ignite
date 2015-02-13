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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.ignitefs.mapreduce.*;
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
     * Gets all GGFS instances.
     *
     * @return Collection of GGFS instances.
     */
    public abstract Collection<IgniteFs> ggfss();

    /**
     * Gets GGFS instance.
     *
     * @param name (Nullable) GGFS name.
     * @return GGFS instance.
     */
    @Nullable public abstract IgniteFs ggfs(@Nullable String name);

    /**
     * Gets server endpoints for particular GGFS.
     *
     * @param name GGFS name.
     * @return Collection of endpoints or {@code null} in case GGFS is not defined.
     */
    public abstract Collection<IpcServerEndpoint> endpoints(@Nullable String name);

    /**
     * Create compute job for the given GGFS job.
     *
     * @param job GGFS job.
     * @param ggfsName GGFS name.
     * @param path Path.
     * @param start Start position.
     * @param length Length.
     * @param recRslv Record resolver.
     * @return Compute job.
     */
    @Nullable public abstract ComputeJob createJob(IgfsJob job, @Nullable String ggfsName, IgniteFsPath path,
        long start, long length, IgfsRecordResolver recRslv);
}
