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

package org.apache.ignite.igfs.mapreduce;

import java.io.IOException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.internal.util.GridFixedSizeInputStream;

/**
 * Convenient {@link IgfsJob} adapter. It limits data returned from {@link IgfsInputStream} to bytes within
 * the {@link IgfsFileRange} assigned to the job.
 * <p>
 * Under the covers it simply puts job's {@code IgfsInputStream} position to range start and wraps in into
 * {@link GridFixedSizeInputStream} limited to range length.
 */
public abstract class IgfsInputStreamJobAdapter extends IgfsJobAdapter {
    /** {@inheritDoc} */
    @Override public final Object execute(IgniteFileSystem igfs, IgfsFileRange range, IgfsInputStream in)
        throws IgniteException, IOException {
        in.seek(range.start());

        return execute(igfs, new IgfsRangeInputStream(in, range));
    }

    /**
     * Executes this job.
     *
     * @param igfs IGFS instance.
     * @param in Input stream.
     * @return Execution result.
     * @throws IgniteException If execution failed.
     * @throws IOException If IO exception encountered while working with stream.
     */
    public abstract Object execute(IgniteFileSystem igfs, IgfsRangeInputStream in) throws IgniteException, IOException;
}