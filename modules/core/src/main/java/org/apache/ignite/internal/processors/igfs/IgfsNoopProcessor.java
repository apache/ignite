/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.IgniteCheckedException;
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
        X.println(">>> IGFS processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   igfsCacheSize: " + 0);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> igfss() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfs(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IpcServerEndpoint> endpoints(String name) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ComputeJob createJob(IgfsJob job, @Nullable String igfsName, IgfsPath path,
        long start, long length, IgfsRecordResolver recRslv) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        // No-op
    }
}