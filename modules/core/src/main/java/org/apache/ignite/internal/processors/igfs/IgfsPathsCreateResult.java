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

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.List;

/**
 * IGFS paths create result.
 */
public class IgfsPathsCreateResult {
    /** Created paths. */
    private final List<IgfsPath> paths;

    /** Info of the last created file. */
    private final IgfsEntryInfo info;

    /**
     * Constructor.
     *
     * @param paths Created paths.
     * @param info Info of the last created file.
     */
    public IgfsPathsCreateResult(List<IgfsPath> paths, IgfsEntryInfo info) {
        this.paths = paths;
        this.info = info;
    }

    /**
     * @return Created paths.
     */
    public List<IgfsPath> createdPaths() {
        return paths;
    }

    /**
     * @return Info of the last created file.
     */
    public IgfsEntryInfo info() {
        return info;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsPathsCreateResult.class, this);
    }
}
