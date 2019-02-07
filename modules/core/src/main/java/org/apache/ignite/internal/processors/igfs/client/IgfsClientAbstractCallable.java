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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract callable for IGFS tasks initiated on client node and passed to data node.
 */
public abstract class IgfsClientAbstractCallable<T> implements IgniteCallable<T>, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    protected String igfsName;

    /** Path for operation. */
    protected IgfsPath path;

    /** User name. */
    protected String user;

    /** Injected instance. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Default constructor.
     */
    protected IgfsClientAbstractCallable() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     * @param user IGFS user name.
     * @param path Path.
     */
    protected IgfsClientAbstractCallable(@Nullable String igfsName, @Nullable String user, @Nullable IgfsPath path) {
        this.igfsName = igfsName;
        this.path = path;
        this.user = user;
    }

    /** {@inheritDoc} */
    @Override public final T call() throws Exception {
        assert ignite != null;

        final IgfsEx igfs = (IgfsEx)ignite.fileSystem(igfsName);

        if (user != null) {
            return IgfsUserContext.doAs(user, new Callable<T>() {
                @Override public T call() throws Exception {
                    return call0(igfs.context());
                }
            });
        } else
            return call0(igfs.context());
    }

    /**
     * Execute task.
     *
     * @param ctx IGFS ocntext.
     * @return Result.
     * @throws Exception If failed.
     */
    protected abstract T call0(IgfsContext ctx) throws Exception;

    /** {@inheritDoc} */
    @Override public final void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeString(igfsName);
        rawWriter.writeString(user);
        IgfsUtils.writePath(rawWriter, path);

        writeBinary0(rawWriter);
    }

    /** {@inheritDoc} */
    @Override public final void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        igfsName = rawReader.readString();
        user = rawReader.readString();
        path = IgfsUtils.readPath(rawReader);

        readBinary0(rawReader);
    }

    /**
     * Write binary.
     *
     * @param rawWriter Raw writer.
     */
    protected void writeBinary0(BinaryRawWriter rawWriter) {
        // No-op.
    }

    /**
     * Read binary.
     *
     * @param rawReader Raw reader.
     */
    protected void readBinary0(BinaryRawReader rawReader) {
        // No-op.
    }
}
