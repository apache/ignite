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

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 * Nop Ignite file system processor implementation.
 */
public class IgfsNoopProcessor extends IgfsProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IgfsNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> IGFS processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   igfsCacheSize: " + 0);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> igfss() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfs(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IpcServerEndpoint> endpoints(@Nullable String name) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ComputeJob createJob(IgfsJob job, @Nullable String igfsName, IgfsPath path,
        long start, long length, IgfsRecordResolver recRslv) {
        return null;
    }
}