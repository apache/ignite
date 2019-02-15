/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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