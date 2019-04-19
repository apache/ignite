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

package org.apache.ignite.internal.processors.igfs.client;

import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * IGFS client list files callable.
 */
public class IgfsClientListFilesCallable extends IgfsClientAbstractCallable<Collection<IgfsFile>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public IgfsClientListFilesCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param user IGFS user name.
     * @param path Path.
     */
    public IgfsClientListFilesCallable(@Nullable String igfsName, @Nullable String user, IgfsPath path) {
        super(igfsName, user, path);
    }

    /** {@inheritDoc} */
    @Override protected Collection<IgfsFile> call0(IgfsContext ctx) throws Exception {
        return ctx.igfs().listFiles(path);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientListFilesCallable.class, this);
    }
}
