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

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.processors.igfs.IgfsContext;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
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
     */
    protected IgfsClientAbstractCallable(@Nullable String igfsName) {
        this.igfsName = igfsName;
    }

    /** {@inheritDoc} */
    @Override public final T call() throws Exception {
        assert ignite != null;

        IgfsEx igfs = (IgfsEx)ignite.fileSystem(igfsName);

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

        writeBinary0(rawWriter);
    }

    /** {@inheritDoc} */
    @Override public final void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        igfsName = rawReader.readString();

        readBinary0(rawReader);
    }

    /**
     * Write binary.
     *
     * @param rawWriter Raw writer.
     */
    protected abstract void writeBinary0(BinaryRawWriter rawWriter);

    /**
     * Read binary.
     *
     * @param rawReader Raw reader.
     */
    protected abstract void readBinary0(BinaryRawReader rawReader);
}
