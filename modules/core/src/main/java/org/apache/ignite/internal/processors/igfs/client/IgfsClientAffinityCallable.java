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
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * IGFS client affinity callable.
 */
public class IgfsClientAffinityCallable extends IgfsClientAbstractCallable<Collection<IgfsBlockLocation>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Start. */
    private long start;

    /** Length. */
    private long len;

    /** Maximum length. */
    private long maxLen;

    /**
     * Default constructor.
     */
    public IgfsClientAffinityCallable() {
        // NO-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param path Path.
     * @param start Start.
     * @param len Length.
     * @param maxLen Maximum length.
     */
    public IgfsClientAffinityCallable(@Nullable String igfsName, IgfsPath path, long start, long len, long maxLen) {
        super(igfsName, path);

        this.start = start;
        this.len = len;
        this.maxLen = maxLen;
    }

    /** {@inheritDoc} */
    @Override protected Collection<IgfsBlockLocation> call0(IgfsContext ctx) throws Exception {
        return ctx.igfs().affinity(path, start, len, maxLen);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary0(BinaryRawWriter writer) throws BinaryObjectException {
        writer.writeLong(start);
        writer.writeLong(len);
        writer.writeLong(maxLen);
    }

    /** {@inheritDoc} */
    @Override public void readBinary0(BinaryRawReader reader) throws BinaryObjectException {
        start = reader.readLong();
        len = reader.readLong();
        maxLen = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsClientAffinityCallable.class, this);
    }
}

