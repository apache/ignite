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

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.internal.GridInternalWrapper;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * IGFS job implementation.
 */
public class IgfsJobImpl implements ComputeJob, GridInternalWrapper<IgfsJob> {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS job. */
    private IgfsJob job;

    /** IGFS name. */
    private String igfsName;

    /** IGFS path. */
    private IgfsPath path;

    /** Start. */
    private long start;

    /** Length. */
    private long len;

    /** Split resolver. */
    private IgfsRecordResolver rslvr;

    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * @param job IGFS job.
     * @param igfsName IGFS name.
     * @param path Split path.
     * @param start Split start offset.
     * @param len Split length.
     * @param rslvr IGFS split resolver.
     */
    public IgfsJobImpl(IgfsJob job, String igfsName, IgfsPath path, long start, long len,
        IgfsRecordResolver rslvr) {
        this.job = job;
        this.igfsName = igfsName;
        this.path = path;
        this.start = start;
        this.len = len;
        this.rslvr = rslvr;
    }

    /** {@inheritDoc} */
    @Override public Object execute() {
        IgniteFileSystem fs = ignite.fileSystem(igfsName);

        try (IgfsInputStream in = fs.open(path)) {
            IgfsFileRange split = new IgfsFileRange(path, start, len);

            if (rslvr != null) {
                split = rslvr.resolveRecords(fs, in, split);

                if (split == null) {
                    log.warning("No data found for split on local node after resolver is applied " +
                        "[igfsName=" + igfsName + ", path=" + path + ", start=" + start + ", len=" + len + ']');

                    return null;
                }
            }

            in.seek(split.start());

            return job.execute(fs, new IgfsFileRange(path, split.start(), split.length()), in);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to execute IGFS job for file split [igfsName=" + igfsName +
                ", path=" + path + ", start=" + start + ", len=" + len + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        job.cancel();
    }

    /** {@inheritDoc} */
    @Override public IgfsJob userObject() {
        return job;
    }
}