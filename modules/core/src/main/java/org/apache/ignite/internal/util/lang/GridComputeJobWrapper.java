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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.compute.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * Convenient wrapper for grid job. It allows to create a job clone in cases when the same
 * job needs to be cloned to multiple grid nodes during mapping phase of task execution.
 */
public class GridComputeJobWrapper implements ComputeJob, Callable<Object>,
    GridPeerDeployAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ComputeJob job;

    /** Peer deploy aware class. */
    private transient volatile GridPeerDeployAware p;

    /**
     * Creates a wrapper with given grid {@code job}.
     *
     * @param job Job to wrap.
     */
    public GridComputeJobWrapper(ComputeJob job) {
        A.notNull(job, "job");

        this.job = job;
    }

    /**
     * Gets wrapped job.
     *
     * @return Wrapped job.
     */
    public ComputeJob wrappedJob() {
        return job;
    }

    /** {@inheritDoc} */
    @Nullable @Override public final Object call() throws Exception {
        return execute();
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        if (p == null)
            p = U.detectPeerDeployAware(this);

        return p.deployClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        if (p == null)
            p = U.detectPeerDeployAware(this);

        return p.classLoader();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        job.cancel();
    }

    /** {@inheritDoc} */
    @Override public Object execute() {
        return job.execute();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridComputeJobWrapper.class, this);
    }
}
