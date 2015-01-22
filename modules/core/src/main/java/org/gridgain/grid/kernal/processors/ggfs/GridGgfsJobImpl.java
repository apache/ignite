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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.fs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.resources.*;

import java.io.*;

/**
 * GGFS job implementation.
 */
public class GridGgfsJobImpl implements ComputeJob, GridInternalWrapper<IgniteFsJob> {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS job. */
    private IgniteFsJob job;

    /** GGFS name. */
    private String ggfsName;

    /** GGFS path. */
    private IgniteFsPath path;

    /** Start. */
    private long start;

    /** Length. */
    private long len;

    /** Split resolver. */
    private IgniteFsRecordResolver rslvr;

    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Injected logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * @param job GGFS job.
     * @param ggfsName GGFS name.
     * @param path Split path.
     * @param start Split start offset.
     * @param len Split length.
     * @param rslvr GGFS split resolver.
     */
    public GridGgfsJobImpl(IgniteFsJob job, String ggfsName, IgniteFsPath path, long start, long len,
        IgniteFsRecordResolver rslvr) {
        this.job = job;
        this.ggfsName = ggfsName;
        this.path = path;
        this.start = start;
        this.len = len;
        this.rslvr = rslvr;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteCheckedException {
        IgniteFs ggfs = ignite.fileSystem(ggfsName);

        try (IgniteFsInputStream in = ggfs.open(path)) {
            IgniteFsFileRange split = new IgniteFsFileRange(path, start, len);

            if (rslvr != null) {
                split = rslvr.resolveRecords(ggfs, in, split);

                if (split == null) {
                    log.warning("No data found for split on local node after resolver is applied " +
                        "[ggfsName=" + ggfsName + ", path=" + path + ", start=" + start + ", len=" + len + ']');

                    return null;
                }
            }

            in.seek(split.start());

            return job.execute(ggfs, new IgniteFsFileRange(path, split.start(), split.length()), in);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to execute GGFS job for file split [ggfsName=" + ggfsName +
                ", path=" + path + ", start=" + start + ", len=" + len + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        job.cancel();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsJob userObject() {
        return job;
    }
}
