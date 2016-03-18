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

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

import java.util.List;

/**
 * IGFS paths create result.
 */
public class IgfsPathsCreateResult {
    /** Created paths. */
    private final List<IgfsPath> paths;

    /** Info of the last created file. */
    private final IgfsFileInfo info;

    /** Parent ID. */
    private final IgniteUuid parentId;

    /**
     * Constructor.
     *
     * @param paths Created paths.
     * @param info Info of the last created file.
     * @param parentId Parent ID.
     */
    public IgfsPathsCreateResult(List<IgfsPath> paths, IgfsFileInfo info, IgniteUuid parentId) {
        this.paths = paths;
        this.info = info;
        this.parentId = parentId;
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
    public IgfsFileInfo info() {
        return info;
    }

    /**
     * @return Parent ID.
     */
    public IgniteUuid parentId() {
        return parentId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsPathsCreateResult.class, this);
    }
}
