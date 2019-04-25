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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsPath;

/**
 * Exception indicating that file batch processing was cancelled.
 */
public class IgfsFileWorkerBatchCancelledException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Path. */
    private IgfsPath path;

    /**
     * Default constructor.
     */
    public IgfsFileWorkerBatchCancelledException() {
        // No-op.
    }

    public IgfsFileWorkerBatchCancelledException(IgfsPath path) {
        this.path = path;
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        if (path == null)
            return "Asynchronous file processing was cancelled due to node stop.";
        else
            return "Asynchronous file processing was cancelled due to node stop: " + path;
    }
}
