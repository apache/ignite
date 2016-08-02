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

package org.apache.ignite.internal.processors.igfs.client;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS client rename callable.
 */
public class IgfsClientRenameCallable extends IgfsClientAbstractCallable<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Destination path. */
    private IgfsPath destPath;

    /**
     * Default constructor.
     */
    public IgfsClientRenameCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param srcPath Source path.
     * @param destPath Destination path.
     */
    public IgfsClientRenameCallable(@Nullable String igfsName, IgfsPath srcPath, IgfsPath destPath) {
        super(igfsName, srcPath);

        this.destPath = destPath;
    }

    /** {@inheritDoc} */
    @Override protected Void call0(IgfsContext ctx) throws Exception {
        ctx.igfs().rename(path, destPath);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary0(BinaryRawWriter writer) throws BinaryObjectException {
        IgfsUtils.writePath(writer, destPath);
    }

    /** {@inheritDoc} */
    @Override public void readBinary0(BinaryRawReader reader) throws BinaryObjectException {
        destPath = IgfsUtils.readPath(reader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientRenameCallable.class, this);
    }
}
